// ==================== 全局基础配置 ====================
// ⚠️ 环境变量（Cloudflare 后台配置）：
//   WEBHOOK_URL          (文本)
//   WEBHOOK_AUTH_TOKEN   (密钥)
//   GITHUB_TOKEN         (密钥)
//   API_KEY              (密钥)
//   DB                   (D1 绑定名)

const DEFAULT_SETTINGS = {
  repoIntervalMinutes: 5,
  cycleIntervalHours: 8,
  dnd: { enabled: false, start: "23:00", end: "08:00" }
};

const DEFAULT_NOTIFICATION_TEMPLATE = {
  update: {
    title: "📢 项目更新通知",
    content: "{repo_name}\n{url}",
    platform: "GitHub",
    username: "{repo_name}",
    eventLabel: "📢",
    taskType: "项目更新",
    taskStatus: "{tag}",
    filename: "{repo_name}",
    error: "{url}"
  },
  alert: {
    title: "🚨 监控异常告警",
    content: "{repo}\n原因：{reason}\n判定：{judge_reason}",
    platform: "GitHub Monitor",
    username: "System Alert",
    eventLabel: "🚨",
    taskType: "异常通知",
    taskStatus: "Failed",
    filename: "{repo}",
    error: "{reason}"
  }
};

const ALLOWED_TEMPLATE_VARS = {
  update: ['repo', 'repo_name', 'url', 'repo_url', 'tag'],
  alert:  ['repo', 'message', 'reason', 'judge_reason']
};

const ALERT_FAILURE_COUNT = 5;
const RECOVERED_SUCCESS_THRESHOLD = 3;
const ALERTED_AT_EXPIRE_HOURS = 24;

const TIMEOUT_GITHUB = 10000;
const TIMEOUT_WEBHOOK = 8000;

// Webhook 推送失败时的重试上限：每个新版本最多推送 MAX_NOTIFY_ATTEMPTS 次（按巡检周期约 8h 间隔），
// 达到上限后停止，避免 Webhook 长期不可达时无限重复同一条通知。
// 若 Webhook 恢复，下一次成功推送即标记该版本为已通知；如需立即补发可手动触发测试。
const MAX_NOTIFY_ATTEMPTS = 3;

const SANITIZE_REGEX = /[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F\u200B-\u200F\u2028-\u202F\u2060-\u206F\uFEFF]/g;
const VAR_REGEX = /\{(\w+)\}/g;

function sanitizeTemplate(obj) {
  if (typeof obj === 'string') return obj.replace(SANITIZE_REGEX, '');
  if (obj && typeof obj === 'object' && !Array.isArray(obj)) {
    const cleaned = {};
    for (const key of Object.keys(obj)) {
      cleaned[key] = sanitizeTemplate(obj[key]);
    }
    return cleaned;
  }
  return obj;
}

// ==================== 免打扰模式（时间按北京时间 UTC+8 解释） ====================
// 判断当前是否处于免打扰时段；start/end 为 "HH:MM"（北京时间），end<=start 视为跨午夜。
function parseHHMM(str) {
  const m = /^(\d{1,2}):(\d{2})$/.exec(str || "");
  if (!m) return null;
  const h = parseInt(m[1], 10), mi = parseInt(m[2], 10);
  if (h > 23 || mi > 59) return null;
  return h * 60 + mi;
}

// 将当前 UTC 时刻换算为北京分钟数（0~1439），用于免打扰判断
function beijingMinutes(now = new Date()) {
  return (now.getUTCHours() * 60 + now.getUTCMinutes() + 8 * 60) % (24 * 60);
}

function isDndActive(dnd, now = new Date()) {
  if (!dnd || !dnd.enabled) return false;
  const s = parseHHMM(dnd.start);
  const e = parseHHMM(dnd.end);
  if (s === null || e === null || s === e) return false;
  const cur = beijingMinutes(now);
  if (s < e) return cur >= s && cur < e;
  return cur >= s || cur < e;
}

// ==================== D1 表初始化（使用单行 prepare + run） ====================
let dbInitPromise = null;

// schema 版本标记：命中即跳过全部 DDL/PRAGMA 兼容检查（cron 基本每次都是冷启动，
// 这一步能把每次冷启动的 9 条 D1 往返压到 1 条读）。新增迁移时把 SCHEMA_VERSION +1 即可重新执行。
const SCHEMA_VERSION = 1;

async function initDB(db, retries = 3) {
  if (dbInitPromise) {
    try {
      await dbInitPromise;
      return;
    } catch (e) {
      dbInitPromise = null;
    }
  }

  const attempt = async (remaining) => {
    try {
      // 快路径：settings 表不存在（全新库）时该查询会抛错，日志后走完整建表流程。
      // 注意：这里的 DDL 必须保持幂等（CREATE TABLE IF NOT EXISTS / duplicate column 可忽略），
      // 否则标记未写入时会反复重试。数据库整体故障会在此记 warn 并退化为每冷启动全量建表。
      let marker = null;
      try {
        marker = await db.prepare("SELECT value FROM settings WHERE key = 'system:schema_version'").first();
      } catch (e) {
        console.warn("读取 schema 版本标记失败（将执行完整建表流程）", e && e.message);
        marker = null;
      }
      if (marker && Number(marker.value) === SCHEMA_VERSION) {
        dbInitPromise = Promise.resolve();
        return;
      }

      // 全部使用 prepare().run()，每条 SQL 为单行字符串，末尾不带分号
      await db.prepare(`CREATE TABLE IF NOT EXISTS check_state (id INTEGER PRIMARY KEY DEFAULT 1, phase TEXT NOT NULL DEFAULT 'waiting', current_index INTEGER NOT NULL DEFAULT 0, cycle_start_time TEXT, last_repo_check_time TEXT, cycle_end_time TEXT, cycle_repos TEXT, total_repos INTEGER DEFAULT 0, version INTEGER NOT NULL DEFAULT 0)`).run();

      await db.prepare(`CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)`).run();

      await db.prepare(`CREATE TABLE IF NOT EXISTS repos (repo TEXT PRIMARY KEY, custom_url TEXT NOT NULL, note TEXT)`).run();

      await db.prepare(`CREATE TABLE IF NOT EXISTS repo_state (repo TEXT PRIMARY KEY, tag TEXT, etag TEXT, errors_json TEXT, updated_at TEXT NOT NULL DEFAULT (datetime('now')))`).run();

      // 站内通知中心：记录每次巡检观测到的版本（UNIQUE(repo, tag) 天然去重）
      await db.prepare(`CREATE TABLE IF NOT EXISTS release_events (id INTEGER PRIMARY KEY AUTOINCREMENT, repo TEXT NOT NULL, tag TEXT NOT NULL, url TEXT NOT NULL, detected_at TEXT NOT NULL, UNIQUE(repo, tag))`).run();

      await db.prepare(`INSERT OR IGNORE INTO check_state (id) VALUES (1)`).run();

      // 兼容旧表 version 列
      const cols = await db.prepare(`PRAGMA table_info(check_state)`).all();
      const hasVersion = cols.results.some(c => c.name === 'version');
      if (!hasVersion) {
        try {
          await db.prepare(`ALTER TABLE check_state ADD COLUMN version INTEGER NOT NULL DEFAULT 0`).run();
        } catch (e) {
          if (!e.message.includes('duplicate column')) throw e;
        }
      }

      // 兼容旧表 note 列（备注）
      const repoCols = await db.prepare(`PRAGMA table_info(repos)`).all();
      const hasNote = repoCols.results.some(c => c.name === 'note');
      if (!hasNote) {
        try {
          await db.prepare(`ALTER TABLE repos ADD COLUMN note TEXT`).run();
        } catch (e) {
          if (!e.message.includes('duplicate column')) throw e;
        }
      }

      // 兼容旧表 repo_state：新增「已通知版本」与「推送尝试次数」两列
      // last_notified_tag：最近一次成功推送通知的版本（用于去重，避免重复通知）
      // notify_attempts：当前版本推送失败后的重试计数（达到上限即停止，防止无限重复）
      const stateCols = await db.prepare(`PRAGMA table_info(repo_state)`).all();
      const stateColNames = stateCols.results.map(c => c.name);
      if (!stateColNames.includes('last_notified_tag')) {
        try {
          await db.prepare(`ALTER TABLE repo_state ADD COLUMN last_notified_tag TEXT`).run();
        } catch (e) {
          if (!e.message.includes('duplicate column')) throw e;
        }
      }
      if (!stateColNames.includes('notify_attempts')) {
        try {
          await db.prepare(`ALTER TABLE repo_state ADD COLUMN notify_attempts INTEGER NOT NULL DEFAULT 0`).run();
        } catch (e) {
          if (!e.message.includes('duplicate column')) throw e;
        }
      }

      await db.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('system:schema_version', ?1)")
        .bind(String(SCHEMA_VERSION)).run();

      dbInitPromise = Promise.resolve();
      return;
    } catch (e) {
      if (remaining > 0) {
        console.warn(`数据库初始化失败，剩余重试次数 ${remaining}，错误：`, e);
        await new Promise(r => setTimeout(r, 500 * (4 - remaining)));
        return attempt(remaining - 1);
      }
      throw e;
    }
  };

  dbInitPromise = attempt(retries);
  return dbInitPromise;
}

// ==================== check_state 乐观锁操作 ====================
async function getCheckState(db) {
  const row = await db.prepare("SELECT * FROM check_state WHERE id = 1").first();
  if (!row) {
    return {
      phase: "waiting",
      currentIndex: 0,
      cycleStartTime: new Date().toISOString(),
      lastRepoCheckTime: null,
      cycleEndTime: null,
      cycleRepos: [],
      totalRepos: 0,
      version: 0
    };
  }
  return {
    phase: row.phase,
    currentIndex: row.current_index,
    cycleStartTime: row.cycle_start_time,
    lastRepoCheckTime: row.last_repo_check_time,
    cycleEndTime: row.cycle_end_time,
    cycleRepos: row.cycle_repos ? JSON.parse(row.cycle_repos) : [],
    totalRepos: row.total_repos,
    version: row.version
  };
}

async function setCheckState(db, state) {
  const phase = state.phase || 'waiting';
  const currentIndex = typeof state.currentIndex === 'number' ? state.currentIndex : 0;
  const cycleStartTime = state.cycleStartTime || null;
  const lastRepoCheckTime = state.lastRepoCheckTime || null;
  const cycleEndTime = state.cycleEndTime || null;
  const cycleRepos = state.cycleRepos ? JSON.stringify(state.cycleRepos) : null;
  const totalRepos = typeof state.totalRepos === 'number' ? state.totalRepos : 0;
  const version = typeof state.version === 'number' ? state.version : 0;
  const newVersion = version + 1;

  const result = await db.prepare(`UPDATE check_state SET
    phase = ?1,
    current_index = ?2,
    cycle_start_time = ?3,
    last_repo_check_time = ?4,
    cycle_end_time = ?5,
    cycle_repos = ?6,
    total_repos = ?7,
    version = ?8
    WHERE id = 1 AND version = ?9`)
    .bind(phase, currentIndex, cycleStartTime, lastRepoCheckTime, cycleEndTime, cycleRepos, totalRepos, newVersion, version)
    .run();
  if (result.meta.changes === 0) {
    throw new Error("CAS_WRITE_CONFLICT");
  }
}

// initialState：调用方已读过 state 时传入，省掉一轮 getCheckState（cron 每轮可少一次 D1 读）。
// 冲突或任何异常后强制置空，下一轮重读（避免拿旧快照重复 CAS）。
async function withOptimisticLock(db, mutator, maxRetries = 3, initialState = null) {
  let state = initialState;
  for (let i = 0; i < maxRetries; i++) {
    try {
      if (state === null) state = await getCheckState(db);
      const modified = await mutator(state);
      if (modified === null) return;
      await setCheckState(db, modified);
      return;
    } catch (e) {
      state = null;
      if (e.message !== "CAS_WRITE_CONFLICT" || i === maxRetries - 1) throw e;
      await new Promise(r => setTimeout(r, 50 * Math.pow(2, i)));
    }
  }
}

// 原子地获取下一个要检查的仓库（闭包传出 item）
async function tryAdvanceAndGetRepo(db, preState = null) {
  let advancedItem = null;
  await withOptimisticLock(db, (state) => {
    if (state.phase !== 'checking') return null;
    const repos = state.cycleRepos;
    if (!repos || state.currentIndex >= repos.length) {
      state.phase = 'waiting';
      state.cycleEndTime = new Date().toISOString();
      return state;
    }
    advancedItem = repos[state.currentIndex];
    state.currentIndex += 1;
    state.lastRepoCheckTime = new Date().toISOString();
    if (state.currentIndex >= repos.length) {
      state.phase = 'waiting';
      state.cycleEndTime = state.lastRepoCheckTime;
    }
    return state;
  }, 3, preState);
  return advancedItem ? { item: advancedItem } : null;
}

// ==================== 数据访问层 ====================
// getSettings 短 TTL 缓存：同 isolate 内跨请求共享，15 秒内不再查 D1。
// saveSettings 会清缓存，因此本 isolate 内的设置变更能即时被感知；
// getSettings 返回浅拷贝，防止调用方（如 /api/save-settings 内联改字段）改坏缓存本体。
let _settingsCache = null;
let _settingsCacheAt = 0;
const SETTINGS_CACHE_TTL_MS = 15000;
async function getSettings(db) {
  const now = Date.now();
  if (_settingsCache && now - _settingsCacheAt < SETTINGS_CACHE_TTL_MS) {
    return { ..._settingsCache, dnd: { ...(_settingsCache.dnd || {}) } };
  }
  const row = await db.prepare("SELECT value FROM settings WHERE key = 'system:settings'").first();
  let s;
  if (!row) s = { ...DEFAULT_SETTINGS };
  else {
    try { s = { ...DEFAULT_SETTINGS, ...JSON.parse(row.value) }; }
    catch { s = { ...DEFAULT_SETTINGS }; }
  }
  s.dnd = { ...(DEFAULT_SETTINGS.dnd || {}), ...(s.dnd || {}) };
  _settingsCache = s;
  _settingsCacheAt = now;
  return { ...s, dnd: { ...s.dnd } };
}

async function saveSettings(db, settings) {
  await db.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('system:settings', ?)")
    .bind(JSON.stringify(settings)).run();
  _settingsCache = null;
  _settingsCacheAt = 0;
}

async function getNotificationTemplate(db) {
  const defaults = DEFAULT_NOTIFICATION_TEMPLATE;
  const row = await db.prepare("SELECT value FROM settings WHERE key = 'system:notification_template'").first();
  if (!row) return JSON.parse(JSON.stringify(defaults));
  try {
    const parsed = JSON.parse(row.value);
    return sanitizeTemplate({
      update: { ...defaults.update, ...(parsed.update || {}) },
      alert:  { ...defaults.alert,  ...(parsed.alert  || {}) }
    });
  } catch {
    return JSON.parse(JSON.stringify(defaults));
  }
}

async function saveNotificationTemplate(db, template) {
  await db.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('system:notification_template', ?)")
    .bind(JSON.stringify(template)).run();
}

// ==================== 通知通道（配置驱动单通道，按部署隔离） ====================
async function getNotifyChannel(db) {
  const row = await db.prepare("SELECT value FROM settings WHERE key = 'system:notify_channel'").first();
  if (!row) return null;
  try { return sanitizeNotifyChannel(JSON.parse(row.value)); }
  catch { return null; }
}
async function saveNotifyChannel(db, channel) {
  const clean = sanitizeNotifyChannel(channel);
  if (!clean) return false;
  await db.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES ('system:notify_channel', ?)").bind(JSON.stringify(clean)).run();
  return true;
}
function sanitizeNotifyChannel(raw) {
  if (!raw || typeof raw !== 'object') return null;
  const url = typeof raw.url === 'string' ? raw.url.trim() : '';
  if (!/^https?:\/\//i.test(url)) return null;            // 必须是 http(s) 绝对地址
  const method = (typeof raw.method === 'string' ? raw.method.toUpperCase() : 'POST');
  if (!['GET','POST','PUT','PATCH','DELETE'].includes(method)) return null;
  const headers = {};
  if (raw.headers && typeof raw.headers === 'object') {
    for (const [k, v] of Object.entries(raw.headers)) {
      if (typeof k === 'string' && (typeof v === 'string' || typeof v === 'number')) headers[k] = String(v);
    }
  }
  if (!headers['Content-Type'] && !headers['content-type']) headers['Content-Type'] = 'application/json';
  const bodyTemplate = typeof raw.bodyTemplate === 'string' && raw.bodyTemplate.length > 0
    ? raw.bodyTemplate
    : '{"title":"{title}","content":"{content}"}';
  const enabled = raw.enabled !== false;
  return { type: 'custom_http', enabled, url, method, headers, bodyTemplate };
}

// ==================== 站内通知中心渠道（release_events + 开关） ====================
// 语义约定（重要）：
//   1) release_events 的写入「不受」 system:inapp_channel 开关影响——只要巡检观测到版本就记录，
//      保证数据完整；用户关掉站内渠道后再打开，仍能看到这段时间累积的更新。
//   2) 该开关「只」控制前端的铃铛入口 / 未读 badge / 顶部更新横幅是否展示，不参与任何推送链路。
const RELEASE_EVENTS_KEEP = 200;
const INAPP_CHANNEL_KEY = 'system:inapp_channel';

function sanitizeInappChannel(raw) {
  return { enabled: !raw || raw.enabled !== false };
}

async function getInappChannel(db) {
  const row = await db.prepare("SELECT value FROM settings WHERE key = ?").bind(INAPP_CHANNEL_KEY).first();
  if (!row) return { enabled: true };
  try { return sanitizeInappChannel(JSON.parse(row.value)); }
  catch { return { enabled: true }; }
}

async function saveInappChannel(db, val) {
  const clean = sanitizeInappChannel(val);
  await db.prepare("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)")
    .bind(INAPP_CHANNEL_KEY, JSON.stringify(clean)).run();
  return clean;
}

// 记录一次版本更新事件。只由 checkSingleRepo 在「真的检测到新版本」时调用；
// 同一 (repo, tag) 靠 release_events 的 UNIQUE 约束天然去重，重复调用不会叠加。
// 整函数 try/catch：失败只打日志，绝不影响巡检主流程。
// 只认 GitHub Release 详情页（形如 https://github.com/owner/repo/releases/...）
const RELEASE_URL_RE = /^https:\/\/github\.com\/[^/]+\/[^/]+\/releases\//i;
function isReleaseUrl(u) { return typeof u === 'string' && RELEASE_URL_RE.test(u); }

async function recordReleaseEvent(db, repo, tag, url) {
  try {
    const repoStr = String(repo || '');
    const tagStr = String(tag || '');
    const res = await db.prepare("INSERT OR IGNORE INTO release_events (repo, tag, url, detected_at) VALUES (?1, ?2, ?3, ?4)")
      .bind(repoStr, tagStr, String(url || ''), new Date().toISOString()).run();
    // 仅在实际插入了新行时才裁剪（取不到 meta.changes 时保守地每次都裁剪）
    const inserted = !(res && res.meta && typeof res.meta.changes === 'number') || res.meta.changes > 0;
    if (inserted) {
      await db.prepare("DELETE FROM release_events WHERE id NOT IN (SELECT id FROM release_events ORDER BY id DESC LIMIT " + RELEASE_EVENTS_KEEP + ")").run();
    } else if (isReleaseUrl(url)) {
      // 同一 (repo, tag) 已存在（304 命中缓存时首次落库可能存的是仓库首页）：
      // 本次拿到了真正的 Release 详情页而库里存的不是，则把链接升级补齐。
      const row = await db.prepare("SELECT url FROM release_events WHERE repo = ?1 AND tag = ?2").bind(repoStr, tagStr).first();
      if (row && !isReleaseUrl(row.url)) {
        await db.prepare("UPDATE release_events SET url = ?1 WHERE repo = ?2 AND tag = ?3").bind(String(url), repoStr, tagStr).run();
      }
    }
  } catch (e) {
    console.error("记录 release_events 失败（不影响巡检主流程）", e);
  }
}

async function getStoredRepos(db) {
  const { results } = await db.prepare("SELECT repo, custom_url, note FROM repos").all();
  return (results || []).map(r => ({ ...r, note: r.note || '' }));
}

async function addRepo(db, repo, custom_url, note = '') {
  await db.prepare("INSERT OR IGNORE INTO repos (repo, custom_url, note) VALUES (?1, ?2, ?3)")
    .bind(repo, custom_url, note).run();
}

async function deleteRepo(db, repo) {
  // 连 release_events 一起清：否则已删仓库的历史通知会永远卡在通知中心
  await db.batch([
    db.prepare("DELETE FROM repos WHERE repo = ?").bind(repo),
    db.prepare("DELETE FROM repo_state WHERE repo = ?").bind(repo),
    db.prepare("DELETE FROM release_events WHERE repo = ?").bind(repo)
  ]);
}

// repo_state 读写统一走「整行读 / 合并写」，把一次巡检的 5 次读 + 最多 6 次写压到 1~2 条 SQL。
// D1 单次往返是本项目最贵的操作（延迟 + 配额 + 失败面），能合并的绝不拆开。
const STATE_EMPTY = { tag: null, etag: null, errors: null, notifiedTag: null, attempts: 0 };

async function getRepoStateRow(db, repo) {
  const row = await db.prepare("SELECT tag, etag, errors_json, last_notified_tag, notify_attempts FROM repo_state WHERE repo = ?").bind(repo).first();
  if (!row) return { ...STATE_EMPTY };
  let errors = null;
  if (row.errors_json) {
    try { errors = JSON.parse(row.errors_json); } catch (e) { errors = null; }
  }
  return {
    tag: row.tag || null,
    etag: row.etag || null,
    errors,
    notifiedTag: row.last_notified_tag || null,
    attempts: row.notify_attempts || 0
  };
}

// patch 只允许 repo_state 的已知列；errors_json 传对象自动序列化，传 null 表示清空。
const STATE_WRITE_COLS = ['tag', 'etag', 'errors_json', 'last_notified_tag', 'notify_attempts'];
async function writeRepoState(db, repo, patch) {
  const cols = [];
  const vals = [];
  for (const c of STATE_WRITE_COLS) {
    if (!(c in patch)) continue;
    const v = patch[c];
    cols.push(c);
    vals.push(c === 'errors_json'
      ? (v === null || v === undefined ? null : JSON.stringify(v))
      : (v === undefined ? null : v));
  }
  if (!cols.length) return;
  const n = cols.length;
  const marks = cols.map((_, i) => '?' + (i + 1)).join(', ');
  const sets = cols.map(c => c + ' = excluded.' + c).join(', ');
  await db.prepare(`INSERT INTO repo_state (repo, ${cols.join(', ')}) VALUES (?${n + 1}, ${marks}) ON CONFLICT(repo) DO UPDATE SET ${sets}`)
    .bind(...vals, repo).run();
}

async function incNotifyAttempts(db, repo) {
  await db.prepare(`INSERT INTO repo_state (repo, notify_attempts) VALUES (?1, 1)
    ON CONFLICT(repo) DO UPDATE SET notify_attempts = notify_attempts + 1`).bind(repo).run();
}

async function getErrorsMap(db) {
  const { results } = await db.prepare("SELECT repo, errors_json FROM repo_state WHERE errors_json IS NOT NULL").all();
  const map = {};
  for (const r of results) {
    try { map[r.repo] = JSON.parse(r.errors_json); } catch {}
  }
  return map;
}

// ==================== Worker 入口 ====================
let lastTestTime = 0;
export default {
  async scheduled(event, env, ctx) {
    const db = env.DB;
    try {
      await initDB(db);
    } catch (e) {
      console.error("定时任务初始化失败", e);
      return;
    }
    // 巡检内部异常必须落日志：否则 waitUntil 里的拒绝会变成静默失败，线上看不出任何痕迹
    ctx.waitUntil(performScheduledCheck(env).catch((e) => console.error("定时巡检异常", e)));
  },

  async fetch(request, env, ctx) {
    const db = env.DB;
    const url = new URL(request.url);

    // 尝试初始化数据库，若失败则根据路径返回错误或面板
    let dbReady = false;
    let initError = null;
    try {
      await initDB(db);
      dbReady = true;
    } catch (e) {
      initError = e;
    }

    // API 请求必须数据库可用
    if (url.pathname.startsWith("/api/")) {
      if (!dbReady) {
        const errorMsg = initError ? initError.message : "Database unavailable";
        return new Response(JSON.stringify({ error: errorMsg }), {
          status: 503,
          headers: { "Content-Type": "application/json" }
        });
      }

      const providedKey = request.headers.get("X-API-Key") || url.searchParams.get("key");
      if (!providedKey || providedKey !== env.API_KEY) {
        return jsonResponse({ error: "Unauthorized" }, 403);
      }
    }

    // 解析请求体（仅 POST/PUT）
    let body = null;
    if (request.method === "POST" || request.method === "PUT") {
      try {
        const text = await request.text();
        if (text.length > 65536) return jsonResponse({ error: "Request body too large" }, 413);
        if (text.length > 0) body = JSON.parse(text);
      } catch (err) {
        if (err instanceof SyntaxError) return jsonResponse({ error: "Invalid JSON" }, 400);
        throw err;
      }
    }

    // ---- 路由 ----
    // 设置
    if (url.pathname === "/api/get-settings") {
      try {
        const [settings, state] = await Promise.all([getSettings(db), getCheckState(db)]);
        return jsonResponse({ settings, state });
      } catch (e) {
        return jsonResponse({ error: "D1 状态读取失败: " + e.message }, 500);
      }
    }

    if (url.pathname === "/api/save-settings" && request.method === "POST") {
      if (!body) return jsonResponse({ error: "Missing body" }, 400);
      const settings = await getSettings(db);
      if (body.repoIntervalMinutes !== undefined) {
        const v = parseInt(body.repoIntervalMinutes, 10);
        if (isNaN(v) || v < 5 || v > 60) return jsonResponse({ success: false, error: "仓库间隔需在 5~60 分钟之间" }, 400);
        settings.repoIntervalMinutes = v;
      }
      if (body.cycleIntervalHours !== undefined) {
        const v = parseInt(body.cycleIntervalHours, 10);
        if (isNaN(v) || v < 1 || v > 48) return jsonResponse({ success: false, error: "周期间隔需在 1~48 小时之间" }, 400);
        settings.cycleIntervalHours = v;
      }
      if (body.dnd !== undefined) {
        const d = body.dnd;
        if (typeof d !== 'object' || d === null) return jsonResponse({ success: false, error: "dnd 必须为对象" }, 400);
        const dnd = { ...(settings.dnd || { enabled: false, start: "23:00", end: "08:00" }) };
        if (d.enabled !== undefined) {
          if (typeof d.enabled !== 'boolean') return jsonResponse({ success: false, error: "dnd.enabled 必须为布尔值" }, 400);
          dnd.enabled = d.enabled;
        }
        if (d.start !== undefined) {
          if (!/^\d{1,2}:\d{2}$/.test(d.start)) return jsonResponse({ success: false, error: "dnd.start 格式应为 HH:MM（北京时间）" }, 400);
          const [sh, sm] = d.start.split(':').map(Number);
          if (sh > 23 || sm > 59) return jsonResponse({ success: false, error: "dnd.start 时间超出范围" }, 400);
          dnd.start = d.start;
        }
        if (d.end !== undefined) {
          if (!/^\d{1,2}:\d{2}$/.test(d.end)) return jsonResponse({ success: false, error: "dnd.end 格式应为 HH:MM（北京时间）" }, 400);
          const [eh, em] = d.end.split(':').map(Number);
          if (eh > 23 || em > 59) return jsonResponse({ success: false, error: "dnd.end 时间超出范围" }, 400);
          dnd.end = d.end;
        }
        settings.dnd = dnd;
      }
      await saveSettings(db, settings);
      return jsonResponse({ success: true, settings });
    }

    // 通知模板
    if (url.pathname === "/api/get-notification-config") {
      const config = await getNotificationTemplate(db);
      return jsonResponse(config);
    }

    if (url.pathname === "/api/save-notification-config" && request.method === "POST") {
      if (!body) return jsonResponse({ error: "Missing body" }, 400);
      if (!body.update || !body.alert) return jsonResponse({ success: false, error: "配置必须包含 update 和 alert 对象" }, 400);

      const MAX_FIELDS = 20;
      const MAX_FIELD_LENGTH = 500;

      for (const section of ['update', 'alert']) {
        const sectionObj = body[section];
        if (!sectionObj || typeof sectionObj !== 'object') return jsonResponse({ success: false, error: `${section} 必须为对象` }, 400);
        const keys = Object.keys(sectionObj);
        if (keys.length > MAX_FIELDS) return jsonResponse({ success: false, error: `${section} 字段数量不能超过 ${MAX_FIELDS}` }, 400);
        for (const [k, v] of Object.entries(sectionObj)) {
          if (typeof v !== 'string') return jsonResponse({ success: false, error: `${section}.${k} 必须为字符串` }, 400);
          if (v.length > MAX_FIELD_LENGTH) return jsonResponse({ success: false, error: `${section}.${k} 长度不能超过 ${MAX_FIELD_LENGTH} 字符` }, 400);
        }
      }

      for (const section of ['update', 'alert']) {
        for (const [k, v] of Object.entries(body[section])) {
          const matches = v.matchAll(VAR_REGEX);
          for (const m of matches) {
            if (!ALLOWED_TEMPLATE_VARS[section].includes(m[1])) {
              return jsonResponse({ success: false, error: `不允许的变量 {${m[1]}} 在 ${section}.${k} 中` }, 400);
            }
          }
        }
      }

      const cleaned = sanitizeTemplate(body);
      await saveNotificationTemplate(db, cleaned);
      const merged = {
        update: { ...DEFAULT_NOTIFICATION_TEMPLATE.update, ...cleaned.update },
        alert:  { ...DEFAULT_NOTIFICATION_TEMPLATE.alert,  ...cleaned.alert }
      };
      return jsonResponse({ success: true, config: merged });
    }

    // 通知通道（配置驱动单通道）
    if (url.pathname === "/api/get-notify-channel") {
      const ch = await getNotifyChannel(db);
      let masked = null;
      if (ch) {
        masked = { ...ch };
        if (masked.headers && masked.headers['Authorization']) {
          masked.headers = { ...masked.headers, Authorization: '***' };
        }
      }
      return jsonResponse({ channel: masked });
    }

    if (url.pathname === "/api/save-notify-channel" && request.method === "POST") {
      if (!body) return jsonResponse({ error: "Missing body" }, 400);
      const clean = sanitizeNotifyChannel(body);
      if (!clean) return jsonResponse({ success: false, error: "无效的通知通道配置：url 须为 http(s) 绝对地址，method 须为 GET/POST/PUT/PATCH/DELETE" }, 400);
      await saveNotifyChannel(db, clean);
      const outHeaders = clean.headers.Authorization ? { ...clean.headers, Authorization: '***' } : clean.headers;
      return jsonResponse({ success: true, channel: { ...clean, headers: outHeaders } });
    }

    // 站内通知中心开关：只控制前端入口/横幅是否展示，release_events 始终照常记录
    if (url.pathname === "/api/get-inapp-channel") {
      const channel = await getInappChannel(db);
      return jsonResponse({ channel });
    }

    if (url.pathname === "/api/save-inapp-channel" && request.method === "POST") {
      if (!body) return jsonResponse({ error: "Missing body" }, 400);
      if (body.enabled !== undefined && typeof body.enabled !== 'boolean') {
        return jsonResponse({ success: false, error: "enabled 必须为布尔值" }, 400);
      }
      const channel = await saveInappChannel(db, body);
      return jsonResponse({ success: true, channel });
    }

    // 仓库管理
    if (url.pathname === "/api/get-repos") {
      const repos = await getStoredRepos(db);
      const errorsMap = await getErrorsMap(db);
      const enriched = repos.map(item => {
        const err = errorsMap[item.repo];
        let health = "ok", lastError = "", reason = "", judgeReason = "";
        if (err) {
          if (err.permanent) health = "dead";
          else if (err.count > 0) health = "warning";
          else if (err.alertedAt) health = "recovered";
          lastError = err.lastError || "";
          reason = err.lastReason || "";
          judgeReason = err.judgeReason || "";
        }
        return { ...item, health, lastError, reason, judgeReason };
      });
      return jsonResponse(enriched);
    }

    // 站内通知中心：最近的版本更新事件（只读，按 id 倒序）
    if (url.pathname === "/api/get-updates") {
      try {
        const { results } = await db.prepare("SELECT id, repo, tag, url, detected_at FROM release_events ORDER BY id DESC LIMIT 100").all();
        return jsonResponse({ updates: (results || []).map(r => ({ id: r.id, repo: r.repo, tag: r.tag, url: r.url, detected_at: r.detected_at })) });
      } catch (e) {
        return jsonResponse({ error: "读取更新事件失败: " + e.message }, 500);
      }
    }

    if (url.pathname === "/api/add-repo" && request.method === "POST") {
      if (!body) return jsonResponse({ error: "Missing body" }, 400);
      const { repo, note } = body;
      if (!repo || !/^[a-zA-Z0-9_.-]+\/[a-zA-Z0-9_.-]+$/.test(repo) || repo.includes("..")) {
        return jsonResponse({ success: false, error: "格式应为「作者/项目名」" }, 400);
      }
      const repos = await getStoredRepos(db);
      if (repos.some(item => item.repo.toLowerCase() === repo.toLowerCase())) {
        return jsonResponse({ success: false, error: "项目已在监控中" }, 400);
      }
      const custom_url = `https://github.com/${repo}/releases`;
      await addRepo(db, repo, custom_url, note || '');
      const updated = await getStoredRepos(db);
      return jsonResponse({ success: true, repos: updated });
    }

    if (url.pathname === "/api/update-note" && request.method === "POST") {
      if (!body) return jsonResponse({ error: "Missing body" }, 400);
      const { repo, note } = body;
      if (!repo) return jsonResponse({ success: false, error: "缺少 repo 参数" }, 400);
      const repos = await getStoredRepos(db);
      if (!repos.some(item => item.repo === repo)) {
        return jsonResponse({ success: false, error: "项目不存在" }, 404);
      }
      await db.prepare("UPDATE repos SET note = ?1 WHERE repo = ?2").bind(note || '', repo).run();
      return jsonResponse({ success: true });
    }

    if (url.pathname === "/api/delete-repo" && request.method === "POST") {
      if (!body) return jsonResponse({ error: "Missing body" }, 400);
      const { repo } = body;
      if (!repo) return jsonResponse({ success: false, error: "缺少 repo 参数" }, 400);
      let repos = await getStoredRepos(db);
      if (!repos.some(item => item.repo === repo)) {
        return jsonResponse({ success: false, error: "项目不存在" }, 404);
      }
      await deleteRepo(db, repo);

      try {
        await withOptimisticLock(db, (state) => {
          if (!state.cycleRepos) return null;
          const idx = state.cycleRepos.findIndex(r => r.repo === repo);
          if (idx === -1) return null;
          state.cycleRepos.splice(idx, 1);
          state.totalRepos = state.cycleRepos.length;
          if (idx < state.currentIndex) {
            state.currentIndex--;
          } else if (state.currentIndex >= state.cycleRepos.length) {
            state.currentIndex = state.cycleRepos.length;
            if (state.phase === 'checking') {
              state.phase = 'waiting';
              state.cycleEndTime = new Date().toISOString();
            }
          }
          return state;
        }, 3);
      } catch (e) {
        console.error("删除仓库时同步 check_state 失败（不影响核心删除）", e);
      }

      const updated = await getStoredRepos(db);
      return jsonResponse({ success: true, repos: updated });
    }

    // 批量导出（逐行输出 owner/name 为 TXT 附件）
    if (url.pathname === "/api/export-repos") {
      const repos = await getStoredRepos(db);
      const lines = (repos || []).map(r => r.repo);
      const txt = lines.join("\n") + (lines.length ? "\n" : "");
      return new Response(txt, {
        headers: {
          "Content-Type": "text/plain; charset=utf-8",
          "Content-Disposition": 'attachment; filename="repos_export.txt"'
        }
      });
    }

    // 批量导入（逐行识别 TXT，支持 `owner/name` 或 `owner/name|备注`）
    if (url.pathname === "/api/import-repos" && request.method === "POST") {
      if (!body || typeof body.content !== "string") {
        return jsonResponse({ success: false, error: "缺少文件内容（content 字段）" }, 400);
      }
      const lines = body.content.split(/\r?\n/);
      const existing = new Set((await getStoredRepos(db)).map(r => r.repo.toLowerCase()));
      const seen = new Set();
      const stmts = [];
      let imported = 0, skipped = 0, invalid = 0;
      const invalidLines = [];
      for (const rawLine of lines) {
        const line = rawLine.trim();
        if (!line || line.startsWith("#")) continue;
        let repo = line, note = '';
        const pipe = line.indexOf("|");
        if (pipe > 0) { repo = line.slice(0, pipe).trim(); note = line.slice(pipe + 1).trim(); }
        if (!/^[a-zA-Z0-9_.-]+\/[a-zA-Z0-9_.-]+$/.test(repo) || repo.includes("..")) {
          invalid++;
          if (invalidLines.length < 10) invalidLines.push(rawLine);
          continue;
        }
        const key = repo.toLowerCase();
        if (existing.has(key) || seen.has(key)) { skipped++; continue; }
        seen.add(key);
        stmts.push(db.prepare("INSERT OR IGNORE INTO repos (repo, custom_url, note) VALUES (?1, ?2, ?3)").bind(repo, `https://github.com/${repo}/releases`, note));
        imported++;
      }
      if (stmts.length) await db.batch(stmts);
      const updated = await getStoredRepos(db);
      return jsonResponse({ success: true, imported, skipped, invalid, invalidLines, repos: updated });
    }

    // 手动测试：随机选一个仓库，跑完整「检测 + 发送通知」逻辑（用于排查 bug / 验证整条链路）
    if (url.pathname === "/api/test") {
      if (Date.now() - lastTestTime < 10000) {
        return jsonResponse({ error: "请求过于频繁，请 10 秒后再试" }, 429);
      }
      lastTestTime = Date.now();

      const repos = await getStoredRepos(db);
      if (!repos.length) {
        return jsonResponse({ tested: 0, total: 0, picked: null, results: [], error: "暂无监控仓库，请先添加至少一个仓库" });
      }
      // 随机选一个仓库，确保每次点测试都覆盖不同仓库，更利于发现潜在 bug
      const idx = Math.floor(Math.random() * repos.length);
      const item = repos[idx];
      const res = await checkSingleRepo(env, item, true);
      const result = {
        repo: res.repo,
        success: res.success,
        push_ok: res.push_ok === true,
        is_new: !!res.is_new,
        dnd_hold: !!res.dnd_hold,
        skipped: !!res.skipped
      };
      return jsonResponse({ tested: 1, total: repos.length, picked: item.repo, results: [result] });
    }

    // 触发新周期
    if (url.pathname === "/api/trigger-cycle" && request.method === "POST") {
      try {
        await startNewCycle(db);
        return jsonResponse({ success: true, message: "已触发新一轮检测" });
      } catch (e) {
        return jsonResponse({ success: false, error: "D1 操作失败: " + e.message }, 500);
      }
    }

    // 已禁用日志
    if (url.pathname === "/api/get-logs") {
      return jsonResponse([]);
    }

    // 默认返回前端面板（即使数据库不可用也能加载）
    return new Response(HTML_TEMPLATE, {
      headers: {
        "Content-Type": "text/html;charset=UTF-8",
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "Content-Security-Policy": "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'"
      }
    });
  }
};

// ==================== 定时检查主逻辑 ====================
async function performScheduledCheck(env) {
  const db = env.DB;

  // state 与 settings 互不依赖，并行读取（每 5 分钟一轮，省下的都是实打实的延迟）
  let state, settings;
  try {
    [state, settings] = await Promise.all([getCheckState(db), getSettings(db)]);
  } catch (e) {
    console.error("读取检查状态/设置失败", e);
    return;
  }

  if (state.phase === "waiting") {
    const now = Date.now();
    const waitSince = state.cycleEndTime
      ? new Date(state.cycleEndTime).getTime()
      : (state.cycleStartTime ? new Date(state.cycleStartTime).getTime() : NaN);
    if (isNaN(waitSince)) {
      console.error('状态异常（cycleStartTime/cycleEndTime 均为 null），强制启动新周期');
      await startNewCycle(db);
      return;
    }
    const cycleMs = settings.cycleIntervalHours * 3600 * 1000;
    if (now - waitSince >= cycleMs) {
      try {
        await startNewCycle(db);
      } catch (e) {
        console.error("启动新周期失败", e);
      }
    }
    return;
  }

  if (state.phase === "checking") {
    if (state.lastRepoCheckTime) {
      const intervalMs = settings.repoIntervalMinutes * 60 * 1000;
      if (Date.now() - new Date(state.lastRepoCheckTime).getTime() < intervalMs - 5000) {
        return;
      }
    }

    // 复用本轮已读的 state 做 CAS，避免 withOptimisticLock 内部再读一次
    const result = await tryAdvanceAndGetRepo(db, state);
    if (!result) return;

    await checkSingleRepo(env, result.item, false, settings.dnd);
  }
}

async function startNewCycle(db) {
  const repos = await getStoredRepos(db);
  await withOptimisticLock(db, (state) => {
    state.phase = "checking";
    state.currentIndex = 0;
    state.cycleStartTime = new Date().toISOString();
    state.lastRepoCheckTime = null;
    state.cycleEndTime = null;
    state.cycleRepos = repos;
    state.totalRepos = repos.length;
    return state;
  }, 3);
}

// ==================== 核心检测逻辑 ====================
async function checkSingleRepo(env, item, forceTrigger, dndSettings) {
  const db = env.DB;
  let repo = item.repo;
  let targetUrl = item.custom_url;
  const now = new Date().toISOString();

  // 免打扰判断：手动测试(forceTrigger)不受限，始终可发送通知
  let dndActive = false;
  if (!forceTrigger) {
    dndActive = isDndActive(dndSettings);
  }

  // 整行读：tag / etag / errors / 已通知版本 / 重试计数 一次拿全（原先是 5 条独立查询）
  const st = forceTrigger ? { ...STATE_EMPTY } : await getRepoStateRow(db, repo);
  let errorInfo = st.errors;

  if (!forceTrigger && errorInfo && errorInfo.permanent) return { repo, success: true, skipped: true };

  const oldTag = forceTrigger ? null : st.tag;
  const githubHeaders = {
    "User-Agent": "CF-Worker-Release-Monitor",
    Accept: "application/vnd.github+json"
  };
  if (env.GITHUB_TOKEN) githubHeaders["Authorization"] = `Bearer ${env.GITHUB_TOKEN}`;

  let data, fromCache = false, res;
  try {
    let etagCache = null;
    if (!forceTrigger && st.etag) {
      try { etagCache = JSON.parse(st.etag); } catch (e) { etagCache = null; }
    }

    const headers = { ...githubHeaders };
    if (etagCache?.etag && oldTag !== null) headers["If-None-Match"] = etagCache.etag;

    for (let attempt = 0; attempt <= 1; attempt++) {
      try {
        res = await fetchWithTimeout(`https://api.github.com/repos/${repo}/releases/latest`, { headers }, TIMEOUT_GITHUB);

        if (res.status === 404) {
          // 免打扰时段内不处理 404（不落库、不告警），待免打扰结束后的下一轮巡检再判定并补发告警，避免被标记 permanent 后永久丢失
          if (!forceTrigger && !dndActive) {
            const cause404 = 'GitHub 返回 404：仓库可能已删除、改名或转为私有';
            errorInfo = {
              count: ALERT_FAILURE_COUNT,
              lastError: cause404,
              lastReason: cause404,
              lastCategory: '404',
              judgeReason: 'GitHub API 返回 404，直接判定为永久失效（状态异常：dead）',
              lastTime: now,
              permanent: true
            };
            await writeRepoState(db, repo, { errors_json: errorInfo });
            const template = await getNotificationTemplate(db);
            await sendAlertNotification(env, db, buildPayload(template.alert, { repo, message: cause404, reason: cause404, judge_reason: errorInfo.judgeReason }));
          }
          return { repo, success: false };
        }

        if (res.status === 304) {
          fromCache = true;
          data = { tag_name: oldTag };
          break;
        }

        if (res.ok) {
          data = await res.json();
          if (!forceTrigger && data.url) {
            const match = data.url.match(/\/repos\/([^/]+\/[^/]+)\/releases\//);
            if (match && match[1] !== repo) {
              return await handleRename(env, repo, match[1], data.tag_name);
            }
          }
          break;
        }

        if (res.status === 403 || res.status === 429 || res.status >= 500) {
          if (attempt < 1) await new Promise(r => setTimeout(r, 1000 * Math.pow(2, attempt)));
          continue;
        }
        throw new Error(`GitHub API ${res.status}`);
      } catch (e) {
        if (attempt === 1 || e.message.startsWith("GitHub API")) throw e;
        if (attempt < 1) await new Promise(r => setTimeout(r, 1000 * Math.pow(2, attempt)));
      }
    }

    if (!data) throw new Error("无法获取 release 信息");
    const latestTag = data.tag_name;

    const isNew = latestTag && latestTag !== oldTag;

    // 写合并：etag / 错误计数衰减 / 观测到新版本 三条 UPSERT 压成一条（推送前的落盘，
    // 保证推送卡死时下一轮不会把同一版本当「全新」重复处理）
    const patch = {};
    if (!forceTrigger && !fromCache && res) patch.etag = JSON.stringify({ etag: res.headers.get("etag") || "" });
    if (!forceTrigger && errorInfo) {
      errorInfo = decayErrorCountForRepo(errorInfo, now);
      patch.errors_json = errorInfo;
    }
    if (!forceTrigger && isNew) {
      patch.tag = latestTag;
      patch.notify_attempts = 0;
    }
    if (Object.keys(patch).length) await writeRepoState(db, repo, patch);

    // 站内通知中心：记录「新版本」事件（release_events 靠 UNIQUE(repo, tag) 去重，重复不会叠加）。
    // 记与不记的语义：
    //   ① 每轮巡检只记「真的新版本」（isNew && oldTag 有值）——否则 32 个仓库跑完一个周期会全部变成「更新」噪音；
    //   ② 手动测试（forceTrigger 时 oldTag 为 null，isNew 即「成功拿到 tag」）也会记录一条事件，
    //      供用户在通知中心查看测试结果；同一仓库同一 tag 反复测试仍被 UNIQUE 去重；
    //   ③ 首次接入的仓库（oldTag 为空且非手动测试）——那属于基线，不是更新，不记录。
    if (isNew && (forceTrigger || oldTag)) {
      let eventUrl;
      const htmlUrl = (data && typeof data.html_url === 'string') ? data.html_url : '';
      if (/^https:\/\/github\.com\//i.test(htmlUrl)) {
        eventUrl = htmlUrl;
      } else if (/^[^/\s]+\/[^/\s]+$/.test(repo)) {
        // 兜底：按 GitHub 固定格式拼 Release 详情页，避免落库成仓库首页导致「查看 Release」跳错
        eventUrl = 'https://github.com/' + repo + '/releases/tag/' + encodeURIComponent(latestTag);
      } else {
        eventUrl = targetUrl;
      }
      await recordReleaseEvent(db, repo, latestTag, eventUrl);
    }
    // 已成功通知的版本（去重依据）；forceTrigger 时不读取，始终强制推送
    const lastNotified = forceTrigger ? null : st.notifiedTag;
    const alreadyNotified = !!latestTag && latestTag === lastNotified;
    // 观测到新版本时上方 UPSERT 已把 notify_attempts 归零，这里等效读取必须为 0，
    // 否则上一版本攒下的失败次数（最高 3）会把这条真新版本直接判为「重试超限」而漏发。
    const attempts = forceTrigger ? 0 : (isNew ? 0 : st.attempts);

    // 仅在「未成功通知过当前版本」且「重试次数未达上限」时才推送；forceTrigger 始终推送
    const needNotify = forceTrigger || (!alreadyNotified && attempts < MAX_NOTIFY_ATTEMPTS);
    if (needNotify) {
      // 免打扰时段：保留已观测到的版本（tag 已记录），不发送；免打扰结束后下一轮检查自动补发
      if (dndActive) {
        return { repo, success: true, dnd_hold: true };
      }
      // 模板与通道互不依赖，并行取，省一次 D1 往返
      const [template, channel] = await Promise.all([getNotificationTemplate(db), getNotifyChannel(db)]);
      const repoName = repo.split("/")[1] || repo;
      const notifVars = { repo, repo_name: repoName, url: targetUrl, repo_url: targetUrl, tag: latestTag || oldTag || "测试" };
      const fields = buildPayload(template.update, notifVars);
      const pushOk = await deliverNotification(env, db, fields, { rawVars: notifVars, channel });

      if (pushOk) {
        // 推送成功：标记该版本已通知并清零重试计数（一条 UPSERT 覆盖原先 2~3 条写）
        const patch2 = { last_notified_tag: latestTag, notify_attempts: 0 };
        if (forceTrigger) patch2.tag = latestTag;
        await writeRepoState(db, repo, patch2);
      } else if (!forceTrigger) {
        // 推送失败：递增重试计数；达到上限后停止，避免无限重复通知
        // 手动测试(forceTrigger)失败不计入重试次数，避免抬高真实巡检的 attempts
        await incNotifyAttempts(db, repo);
      }

      return { repo, success: true, push_ok: pushOk, is_new: !forceTrigger && isNew };
    }

    return { repo, success: true };
  } catch (err) {
    if (!forceTrigger) {
      const cls = classifyError(err, res);
      errorInfo = errorInfo || { count: 0 };
      errorInfo.count = (errorInfo.count || 0) + 1;
      errorInfo.lastError = err.message;
      errorInfo.lastReason = cls.reason;
      errorInfo.lastCategory = cls.category;
      errorInfo.lastTime = now;
      errorInfo.successCount = 0;

      const judgeReason = '连续 ' + errorInfo.count + ' 次检测失败（告警阈值 ' + ALERT_FAILURE_COUNT + '）：' + cls.reason;
      // 免打扰时段内不发送异常告警，也不标记 alertedAt，免打扰结束后下一轮会重试
      if (errorInfo.count >= ALERT_FAILURE_COUNT && !errorInfo.alertedAt && !dndActive) {
        const template = await getNotificationTemplate(db);
        await sendAlertNotification(env, db, buildPayload(template.alert, { repo, message: err.message, reason: cls.reason, judge_reason: judgeReason }));
        errorInfo.alertedAt = now;
      }

      // 落盘失败不能让整轮巡检抛异常：告警已发出，下一轮会重新计数并重试
      try {
        await writeRepoState(db, repo, { errors_json: errorInfo });
      } catch (e2) {
        console.error("保存错误计数失败（不影响本轮返回）", e2);
      }
    }
    return { repo, success: false };
  }
}

// ==================== 错误计数逻辑（返回新对象或 null） ====================
function decayErrorCountForRepo(errorInfo, now) {
  if (!errorInfo || errorInfo.permanent) return errorInfo;

  if (errorInfo.alertedAt) {
    const alertedTime = new Date(errorInfo.alertedAt).getTime();
    if (Date.now() - alertedTime > ALERTED_AT_EXPIRE_HOURS * 3600 * 1000) {
      return { ...errorInfo, alertedAt: null, lastTime: now };
    }
  }

  const newCount = Math.max(0, (errorInfo.count || 0) - 2);
  if (newCount === 0 && !errorInfo.alertedAt) {
    return null;
  } else if (newCount === 0 && errorInfo.alertedAt) {
    const successCount = (errorInfo.successCount || 0) + 1;
    if (successCount >= RECOVERED_SUCCESS_THRESHOLD) {
      return null;
    } else {
      return { ...errorInfo, count: 0, lastError: "", lastReason: "", judgeReason: "", lastTime: now, successCount };
    }
  } else {
    return { ...errorInfo, count: newCount, lastError: "", lastReason: "", judgeReason: "", lastTime: now, successCount: 0 };
  }
}

// ==================== 仓库重命名处理 ====================
async function handleRename(env, oldRepo, newRepo, tag) {
  const db = env.DB;

  await db.batch([
    db.prepare("UPDATE repos SET repo = ?1, custom_url = ?2 WHERE repo = ?3")
      .bind(newRepo, `https://github.com/${newRepo}/releases`, oldRepo),
    db.prepare("INSERT OR IGNORE INTO repo_state (repo, tag, etag, errors_json, last_notified_tag, notify_attempts) SELECT ?1, tag, etag, errors_json, last_notified_tag, notify_attempts FROM repo_state WHERE repo = ?2")
      .bind(newRepo, oldRepo),
    db.prepare("DELETE FROM repo_state WHERE repo = ?").bind(oldRepo),
    // 历史通知跟随改名：先删掉会撞 UNIQUE(repo, tag) 的旧名行，避免残留孤儿数据长期占位
    db.prepare("DELETE FROM release_events WHERE repo = ?2 AND tag IN (SELECT tag FROM release_events WHERE repo = ?1)").bind(newRepo, oldRepo),
    db.prepare("UPDATE OR IGNORE release_events SET repo = ?1 WHERE repo = ?2").bind(newRepo, oldRepo),
    db.prepare("INSERT OR IGNORE INTO repo_state (repo) VALUES (?)").bind(newRepo)
  ]);

  try {
    await withOptimisticLock(db, (state) => {
      if (!state.cycleRepos) return null;
      const repoInSnapshot = state.cycleRepos.find(r => r.repo === oldRepo);
      if (!repoInSnapshot) return null;
      repoInSnapshot.repo = newRepo;
      repoInSnapshot.custom_url = `https://github.com/${newRepo}/releases`;
      return state;
    }, 3);
  } catch (e) {
    console.error("重命名同步 check_state 快照失败", e);
  }

  return { repo: newRepo, success: true };
}

// ==================== 辅助函数 ====================
function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", "X-Content-Type-Options": "nosniff" }
  });
}

function fetchWithTimeout(url, options, timeoutMs) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  return fetch(url, { ...options, signal: controller.signal }).finally(() => clearTimeout(timeout));
}

async function sendAlertNotification(env, db, payload) {
  try { await deliverNotification(env, db, payload, { isAlert: true }); } catch (e) { /* 忽略 */ }
}

async function deliverNotification(env, db, fields, opts = {}) {
  // channel 由调用方预取传入（与通知模板并行取），未传才回查，省一次 D1 往返
  const channel = opts.channel !== undefined ? opts.channel : await getNotifyChannel(db);
  let url, method, headers, bodyStr;
  if (channel && channel.enabled && channel.url) {
    // 走配置驱动通道（别人的/自己的都在各自的 D1 配置里）
    url = channel.url;
    method = channel.method;
    headers = { ...channel.headers };
    const mergedVars = { ...fields, ...(opts.rawVars || {}) };
    bodyStr = substituteVars(channel.bodyTemplate, mergedVars);
  } else {
    // 回退：沿用 WEBHOOK_URL + WEBHOOK_AUTH_TOKEN secret（兼容现有部署）
    url = env.WEBHOOK_URL;
    method = "POST";
    headers = { "Content-Type": "application/json", ...(env.WEBHOOK_AUTH_TOKEN ? { Authorization: env.WEBHOOK_AUTH_TOKEN } : {}) };
    bodyStr = JSON.stringify({ title: fields.title, content: fields.content });
  }
  if (!url) return false;
  let pushOk = false;
  for (let attempt = 0; attempt <= 1; attempt++) {
    try {
      const res = await fetchWithTimeout(url, {
        method,
        headers,
        body: method === "GET" ? undefined : bodyStr
      }, TIMEOUT_WEBHOOK);
      if (res.ok) { pushOk = true; break; }
    } catch (e) {
      if (attempt === 1) break;
    }
    if (attempt < 1) await new Promise(r => setTimeout(r, 1000));
  }
  return pushOk;
}
function substituteVars(template, vars) {
  return template.replace(VAR_REGEX, (_, name) => {
    let val = vars[name] !== undefined ? vars[name] : '{' + name + '}';
    if (typeof val === 'string') val = val.replace(SANITIZE_REGEX, '');
    return val;
  });
}

function buildPayload(template, vars) {
  const payload = {};
  for (const key of Object.keys(template)) {
    let value = template[key];
    if (typeof value === 'string') {
      value = value.replace(VAR_REGEX, (_, varName) => {
        let val = vars[varName] !== undefined ? vars[varName] : `{${varName}}`;
        if (typeof val === 'string') val = val.replace(SANITIZE_REGEX, '');
        return val;
      });
    }
    payload[key] = value;
  }
  return payload;
}

// 把原始异常归类为"导致状态异常的原因"（结构化根因）
function classifyError(err, res) {
  const status = res && res.status ? res.status : null;
  const msg = (err && err.message) ? err.message : '';
  if (status === 404) return { reason: 'GitHub 返回 404：仓库可能已删除、改名或转为私有', category: '404' };
  if (status === 403 || status === 429) return { reason: 'GitHub API 限流或无权限（' + status + '）', category: 'rate_limit' };
  if (status >= 500) return { reason: 'GitHub 服务器错误（' + status + '）', category: 'server_error' };
  if (/fetch|timeout|timed out|network|ECONN|abort|Failed to fetch/i.test(msg)) return { reason: '网络请求失败或超时，无法连接 GitHub', category: 'network' };
  if (/JSON|parse|Unexpected token|SyntaxError/i.test(msg)) return { reason: '响应解析失败，GitHub 返回了非预期内容', category: 'parse' };
  return { reason: msg || '未知错误', category: 'unknown' };
}

// ==================== 完整前端面板 ====================
const FAVICON_B64 = "AAABAAEAQEAAAAEAIAAoQgAAFgAAACgAAABAAAAAgAAAAAEAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAbAAAAP8AAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABgAAAD/AAAAJgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA+gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA4AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAGgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD0AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAKAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAB2AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAA/AAAAP8AAAD8AAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAADAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAAAAAAAAIAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKYAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAHgAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAIIAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAC4AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACGAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAAAAAAAAAAAAD0AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAALIAAAAiAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAASgAAAN4AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAHIAAAAAAAAAAAAAABIAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAJQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAyAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAADgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAOgAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAAAAAAAAAAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAADuAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAAAAAAAQAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAACMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADWAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAADoAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAADaAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAPwAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACYAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAAAAAAAXAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAWAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAHAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAANQAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD6AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA3AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAOQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAACyAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAACwAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAARgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAqgAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAZgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMQAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAAAAAAAAAAAANYAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAADSAAAAAAAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAHAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA4gAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAAAAAAAAAAAABeAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAC4AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAFwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACUAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAACQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAcAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEwAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAaAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAhAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAANYAAACyAAAAtAAAANoAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD6AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAFgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAOwAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAygAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAACWAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAHgAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAUAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAE4AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAC0AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAALIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMQAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAMQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAiAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAIwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAABQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKoAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAArgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAMoAAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAAzgAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACqAAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAAP8AAAD/AAAA/wAAALIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABqAAAAyAAAAP8AAAD/AAAA/wAAAP8AAADIAAAAagAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA//////////////3//7//////4P//B/////+A//8B/////gD//wB////8AP//AD////AA//8AD///4AD//wAH///Af///AAP//4H///8AAf//A////wAA//4D////AAB//Afj//8AAD/8B4D//wAAP/gPAP/+AAAf8B4Af/4AAA/wHAB//AAAD+B4AH/+AAAH4PAH///gAAfAAD////wAA8AAf////gADwAD/////AAOAAf////+AAYAD/////8ABgAf/////4AGAB//////gAQAP/////+AAAA//////8AAAD//////wAAAP//////AAAA//////8AAAD//////wAAAP//////AAAA//////8AAAD//////wAAAP//////AAAA//////8ACAD//////gAYAH/////+ABgAf/////wAGAA//////AAcAB/////4ADwAP/////gAPgA//////AB+AD/////8AH4AP/////wAfwA//////AD/gB/AAAP4Af+AHwAAAPgB/8AYAAAAOAP/4AAAAAAAB//wAAAAAAAP//AAAAAAAA//+AAAAAAAH//8AAAAAAA///8AAAAAAP///4AAAAAB////wAAAAAP////wAAAAD/////wAAAA//////wAAAP//////4AAH////////gf//////////////8=";

const HTML_TEMPLATE = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
  <title>GitHub Release 监控控制台</title>
  <link rel="icon" href="data:image/x-icon;base64,${FAVICON_B64}">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&family=Roboto+Mono:wght@400;500&display=swap" rel="stylesheet">
  <style>
    :root {
      /* ===== MD2 调色板：主色 Blue 700 #1976D2 ===== */
      --md-primary-50:#E3F2FD; --md-primary-100:#BBDEFB; --md-primary-200:#90CAF9; --md-primary-300:#64B5F6; --md-primary-400:#42A5F5; --md-primary-500:#2196F3; --md-primary-600:#1E88E5; --md-primary-700:#1976D2; --md-primary-800:#1565C0; --md-primary-900:#0D47A1;
      --md-primary:#1976D2; --md-on-primary:#FFFFFF; --md-primary-variant:#1565C0; --md-primary-light:#E3F2FD;
      --md-secondary:#00897B; --md-on-secondary:#FFFFFF; --md-secondary-variant:#00796B;
      --md-background:#FAFAFA; --md-surface:#FFFFFF; --md-surface-2:#F5F5F5;
      --md-on-background:#212121; --md-on-surface:#212121; --md-on-surface-medium:#5F6368; --md-on-surface-disabled:rgba(33,33,33,0.38); --md-on-surface-disabled-bg:rgba(33,33,33,0.12);
      --md-divider:#E0E0E0; --md-outline:#BDBDBD; --md-outline-focused:#1976D2;
      --md-error:#D32F2F; --md-on-error:#FFFFFF; --md-error-light:#FDECEA;
      --radius-card:8px; --radius-button:4px; --radius-input:4px; --radius-fab:50%; --radius-chip:4px;
      --space-1:4px; --space-2:8px; --space-3:12px; --space-4:16px; --space-5:20px; --space-6:24px; --space-7:28px; --space-8:32px; --space-9:36px; --space-10:40px; --space-11:44px; --space-12:48px;
      --font-base:"Roboto","Segoe UI",-apple-system,BlinkMacSystemFont,"Helvetica Neue",Arial,"PingFang SC","Microsoft YaHei",sans-serif;
      --font-mono:"Roboto Mono","JetBrains Mono",ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;
      --elev-0:none;
      --elev-1:0 1px 3px rgba(0,0,0,0.12),0 1px 2px rgba(0,0,0,0.24);
      --elev-2:0 3px 6px rgba(0,0,0,0.16),0 3px 6px rgba(0,0,0,0.23);
      --elev-4:0 10px 20px rgba(0,0,0,0.19),0 6px 6px rgba(0,0,0,0.23);
      --elev-6:0 6px 10px rgba(0,0,0,0.16),0 1px 18px rgba(0,0,0,0.22);
      --elev-8:0 14px 28px rgba(0,0,0,0.25),0 10px 10px rgba(0,0,0,0.22);
      --elev-16:0 16px 24px rgba(0,0,0,0.22),0 6px 30px rgba(0,0,0,0.30);
      --elev-24:0 24px 38px rgba(0,0,0,0.25),0 9px 46px rgba(0,0,0,0.12);
      --ease-standard:cubic-bezier(0.4,0,0.2,1); --dur-fast:150ms; --dur-base:200ms;

      /* 兼容旧 inline style 仍引用的 --md-sys-color-* 变量 */
      --md-sys-color-primary:#1976D2; --md-sys-color-on-primary:#FFFFFF;
      --md-sys-color-primary-container:#E3F2FD; --md-sys-color-on-primary-container:#01579B;
      --md-sys-color-secondary:#00897B; --md-sys-color-on-secondary:#FFFFFF;
      --md-sys-color-secondary-container:#E0F2F1; --md-sys-color-on-secondary-container:#004D40;
      --md-sys-color-error:#D32F2F; --md-sys-color-on-error:#FFFFFF;
      --md-sys-color-error-container:#FDECEA; --md-sys-color-on-error-container:#410E0B;
      --md-sys-color-background:#FAFAFA; --md-sys-color-on-background:#212121;
      --md-sys-color-surface:#FFFFFF; --md-sys-color-on-surface:#212121;
      --md-sys-color-surface-variant:#F5F5F5; --md-sys-color-on-surface-variant:#5F6368;
      --md-sys-color-outline:#BDBDBD; --md-sys-color-outline-variant:#E0E0E0;
      --md-sys-color-surface-1:#F5F5F5; --md-sys-color-surface-2:#EEEEEE;
      --md-sys-elevation-1:0 1px 3px rgba(0,0,0,0.12),0 1px 2px rgba(0,0,0,0.24);
      --md-sys-elevation-2:0 3px 6px rgba(0,0,0,0.16),0 3px 6px rgba(0,0,0,0.23);
    }
    @media (prefers-color-scheme: dark) {
      :root {
        --md-background:#121212; --md-surface:#1E1E1E; --md-surface-2:#2A2A2A;
        --md-on-background:#E6E1E5; --md-on-surface:#E6E1E5; --md-on-surface-medium:#B0B0B0;
        --md-on-surface-disabled:rgba(255,255,255,0.38); --md-on-surface-disabled-bg:rgba(255,255,255,0.12);
        --md-divider:#3C3C3C; --md-outline:#5A5A5A; --md-primary-light:#1A3A5C;
        --md-primary:#42A5F5; --md-on-primary:#012A36; --md-primary-variant:#1E88E5; --md-primary-700:#1976D2;
        --md-secondary:#4DB6AC; --md-on-secondary:#012A23;
        --md-error:#CF6679; --md-on-error:#381E1E; --md-error-light:#5C1A17;
        /* 兼容旧 inline style 变量（暗色） */
        --md-sys-color-primary:#42A5F5; --md-sys-color-on-primary:#012A36;
        --md-sys-color-primary-container:#1A3A5C; --md-sys-color-on-primary-container:#A6E7FF;
        --md-sys-color-secondary:#4DB6AC; --md-sys-color-on-secondary:#012A23;
        --md-sys-color-secondary-container:#003B4A; --md-sys-color-on-secondary-container:#A6E7FF;
        --md-sys-color-error:#CF6679; --md-sys-color-on-error:#381E1E;
        --md-sys-color-error-container:#5C1A17; --md-sys-color-on-error-container:#F9DEDC;
        --md-sys-color-background:#121212; --md-sys-color-on-background:#E6E1E5;
        --md-sys-color-surface:#1E1E1E; --md-sys-color-on-surface:#E6E1E5;
        --md-sys-color-surface-variant:#2A2A2A; --md-sys-color-on-surface-variant:#B0B0B0;
        --md-sys-color-outline:#5A5A5A; --md-sys-color-outline-variant:#2A2A2A;
        --md-sys-color-surface-1:#242424; --md-sys-color-surface-2:#2C2C2C;
      }
      .badge-ok { background:#1A3C28; color:#81C995; }
      .badge-warn { background:#3C3014; color:#FDD663; }
      .badge-dead { background:#3C1E1C; color:#F28B82; }
      .badge-recov { background:#3C3414; color:#FDD663; }
      .input, textarea, .note-input { background:#2A2A2A; }
      .app-header { background:#1E1E1E; }
    }
    *, *::before, *::after { box-sizing: border-box; }
    html { font-size: 16px; }
    body {
      font-family: var(--font-base);
      background: var(--md-background);
      color: var(--md-on-background);
      line-height: 1.5;
      padding: var(--space-4);
      max-width: 1020px;
      margin: 0 auto;
    }
    code { background: var(--md-surface-2); padding: 2px 6px; border-radius: 4px; font-family: var(--font-mono); font-size: 12px; }
    /* MD2 AppBar（沿用现有 .app-header 结构，保留内部文字与 emoji） */
    .app-header {
      position: sticky; top: 0; z-index: 100;
      display: flex; align-items: center; justify-content: space-between;
      height: 56px; margin: calc(-1 * var(--space-4)) calc(-1 * var(--space-4)) var(--space-6);
      padding: 0 var(--space-4);
      background: var(--md-surface);
      border-bottom: 1px solid var(--md-divider);
      box-shadow: var(--elev-4);
    }
    .app-header h1 { font-size: 20px; font-weight: 500; line-height: 28px; color: var(--md-on-surface); }
    .version {
      display: inline-block; background: var(--md-primary-light); color: var(--md-primary-700);
      padding: 2px 10px; border-radius: var(--radius-chip); font-size: 0.75rem; font-weight: 500;
    }
    .subtitle { color: var(--md-on-surface-medium); font-size: 0.8125rem; }
    /* MD2 Card：8px 圆角 + elevation 2 静止 / 4 hover，去掉描边 */
    .card {
      background: var(--md-surface); border-radius: var(--radius-card); padding: var(--space-6);
      margin-bottom: var(--space-6); box-shadow: var(--elev-2); border: none;
      transition: box-shadow var(--dur-base) var(--ease-standard), transform var(--dur-base) var(--ease-standard);
    }
    .card:hover { box-shadow: var(--elev-4); transform: translateY(-2px); }
    .card h2 { font-size: 20px; font-weight: 500; line-height: 28px; margin-bottom: var(--space-4); }
    .card h3 { font-size: 16px; font-weight: 500; line-height: 24px; margin-bottom: var(--space-2); }
    .form-group { display: flex; align-items: center; gap: var(--space-4); margin-bottom: var(--space-4); flex-wrap: wrap; }
    .form-group label { min-width: 160px; font-size: 0.875rem; color: var(--md-on-surface-medium); }
    /* MD2 TextField（outlined 风格） */
    .input {
      height: 56px; padding: 0 var(--space-3);
      border: 1px solid var(--md-outline); border-radius: var(--radius-input);
      background: var(--md-surface); color: var(--md-on-surface);
      font-family: var(--font-base); font-size: 14px; line-height: 20px; outline: none;
      transition: border-color var(--dur-fast) var(--ease-standard), box-shadow var(--dur-fast) var(--ease-standard);
    }
    .input::placeholder { color: var(--md-on-surface-medium); }
    .input:hover { border-color: var(--md-on-surface); }
    .input:focus { border-color: var(--md-primary); border-width: 2px; padding: 0 calc(var(--space-3) - 1px); }
    .input:disabled { background: var(--md-surface-2); color: var(--md-on-surface-disabled); border-color: var(--md-divider); }
    input[type="number"] { width: 80px; text-align: center; }
    input[type="text"] { flex: 1; min-width: 200px; }
    select.input { padding-right: var(--space-6); appearance: none; -webkit-appearance: none;
      background-image: url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='24' height='24' viewBox='0 0 24 24'><path fill='%235F6368' d='M7 10l5 5 5-5z'/></svg>");
      background-repeat: no-repeat; background-position: right 8px center; }
    textarea {
      width: 100%; min-height: 180px; font-family: var(--font-mono); font-size: 13px; line-height: 20px;
      padding: var(--space-3); border: 1px solid var(--md-outline); border-radius: var(--radius-input);
      background: var(--md-surface); color: var(--md-on-surface); resize: vertical; outline: none;
    }
    textarea:focus { border-color: var(--md-primary); border-width: 2px; padding: calc(var(--space-3) - 1px); }
    .help-text { font-size: 0.75rem; color: var(--md-on-surface-medium); margin-bottom: var(--space-2); }
    /* MD2 Button 三级（4px 圆角，hover/active/focus-visible/disabled） */
    .btn {
      display: inline-flex; align-items: center; justify-content: center; gap: var(--space-1);
      height: 40px; min-width: 64px; padding: 0 var(--space-6);
      border-radius: var(--radius-button); border: none;
      font-family: var(--font-base); font-size: 14px; font-weight: 500; line-height: 20px; letter-spacing: 0.5px;
      cursor: pointer; user-select: none; position: relative; overflow: hidden;
      transition: box-shadow var(--dur-base) var(--ease-standard), background-color var(--dur-fast) var(--ease-standard), transform var(--dur-fast) var(--ease-standard);
    }
    .btn-filled { background: var(--md-primary); color: var(--md-on-primary); box-shadow: var(--elev-2); }
    .btn-filled:hover { box-shadow: var(--elev-4); }
    .btn-filled:active { background: var(--md-primary-variant); transform: scale(0.97); }
    .btn-tonal { background: var(--md-secondary); color: var(--md-on-secondary); box-shadow: var(--elev-1); }
    .btn-tonal:hover { background: var(--md-secondary-variant); box-shadow: var(--elev-2); }
    .btn-tonal:active { transform: scale(0.97); }
    .btn-outlined { background: transparent; border: 1px solid var(--md-outline); color: var(--md-primary); box-shadow: none; }
    .btn-outlined:hover { background: var(--md-primary-light); border-color: var(--md-primary); }
    .btn-outlined:active { background: var(--md-primary-100); transform: scale(0.97); }
    .btn-error { background: var(--md-error); color: var(--md-on-error); box-shadow: var(--elev-1); }
    .btn-error:hover { box-shadow: var(--elev-2); }
    .btn-error:active { background: #B71C1C; transform: scale(0.97); }
    .btn:focus-visible { outline: 2px solid var(--md-primary); outline-offset: 2px; }
    .btn:disabled, .btn[disabled] { background: var(--md-on-surface-disabled-bg) !important; color: var(--md-on-surface-disabled) !important; box-shadow: none !important; opacity: 1; pointer-events: none; cursor: not-allowed; }
    /* 纯 CSS 水波纹（不依赖 JS，从元素中心扩散） */
    .btn::after, .md-fab::after {
      content: ""; position: absolute; left: 50%; top: 50%;
      width: 8px; height: 8px; border-radius: 50%; background: currentColor; opacity: 0;
      transform: translate(-50%, -50%) scale(1); pointer-events: none;
    }
    .btn:active::after, .md-fab:active::after { animation: md-ripple 480ms var(--ease-standard); }
    @keyframes md-ripple { 0% { opacity: 0.32; transform: translate(-50%, -50%) scale(1); } 100% { opacity: 0; transform: translate(-50%, -50%) scale(28); } }
    /* MD2 Table */
    .table-wrapper { overflow-x: auto; margin: 0 calc(-1 * var(--space-2)); padding: 0 var(--space-2); }
    table { width: 100%; border-collapse: collapse; font-size: 14px; min-width: 600px; }
    th { height: 52px; text-align: left; padding: 0 var(--space-3); border-bottom: 1px solid var(--md-divider); font-size: 12px; font-weight: 500; line-height: 16px; letter-spacing: 0.5px; text-transform: uppercase; color: var(--md-on-surface-medium); }
    td { height: 52px; padding: 0 var(--space-3); border-bottom: 1px solid var(--md-divider); color: var(--md-on-surface); vertical-align: middle; }
    tbody tr { transition: background-color var(--dur-fast) var(--ease-standard); }
    tbody tr:hover { background: var(--md-surface-2); }
    /* MD2 Badge / Chip（4px 小圆角） */
    .badge { display: inline-block; padding: 3px 10px; border-radius: var(--radius-chip); font-size: 11px; font-weight: 500; line-height: 16px; letter-spacing: 0.4px; }
    .badge-ok { background: #E6F4EA; color: #1E7A34; }
    .badge-warn { background: #FEF7E0; color: #B26A00; }
    .badge-dead { background: #FCE8E6; color: #C5221F; }
    .badge-recov { background: #FFFDE7; color: #8A6D00; }
    .note-input { width: 100%; min-width: 120px; box-sizing: border-box; height: 36px; padding: 0 var(--space-2); border: 1px solid var(--md-outline); border-radius: var(--radius-input); background: var(--md-surface); color: var(--md-on-surface); font-size: 13px; outline: none; transition: border-color var(--dur-fast) var(--ease-standard); }
    .note-input:focus { border-color: var(--md-primary); border-width: 2px; padding: 0 calc(var(--space-2) - 1px); }
    .info-panel { background: var(--md-primary-light); color: var(--md-primary-700); padding: 12px 16px; border-radius: var(--radius-card); margin-top: var(--space-4); font-size: 0.85rem; }
    .auth-error { background: var(--md-error-light); color: var(--md-error); padding: 12px 16px; border-radius: var(--radius-card); margin-bottom: var(--space-6); display: none; }
    .result-block { background: var(--md-surface-2); padding: var(--space-4); border-radius: var(--radius-card); font-family: var(--font-mono); font-size: 12px; overflow-x: auto; margin-top: var(--space-4); white-space: pre-wrap; }
    /* MD2 FAB（右下角圆形，承载「添加仓库」快捷入口） */
    .md-fab {
      position: fixed; right: var(--space-6); bottom: var(--space-6);
      width: 56px; height: 56px; border-radius: var(--radius-fab);
      display: flex; align-items: center; justify-content: center;
      background: var(--md-primary); color: var(--md-on-primary); border: none; cursor: pointer;
      font-size: 24px; box-shadow: var(--elev-6); overflow: hidden;
      transition: box-shadow var(--dur-base) var(--ease-standard), transform var(--dur-fast) var(--ease-standard), background-color var(--dur-fast) var(--ease-standard);
      z-index: 200;
    }
    .md-fab:hover { box-shadow: var(--elev-8); background: var(--md-primary-600); }
    .md-fab:active { transform: scale(0.96); background: var(--md-primary-variant); }
    .md-fab:focus-visible { outline: 2px solid var(--md-primary); outline-offset: 3px; }
    @media (max-width: 768px) {
      body { padding: var(--space-2); }
      .card { padding: var(--space-4); margin-bottom: var(--space-4); }
      .form-group { flex-direction: column; align-items: stretch; }
      .btn { width: 100%; }
      .app-header { margin-left: calc(-1 * var(--space-2)); margin-right: calc(-1 * var(--space-2)); }
    }
    @media (max-width: 599px) {
      .md-fab { right: var(--space-4); bottom: var(--space-4); }
    }
    /* ===== AppBar 右侧操作区（铃铛 / 设置） ===== */
    .header-left { display: flex; align-items: baseline; gap: var(--space-3); min-width: 0; flex-wrap: wrap; }
    .header-actions { display: flex; align-items: center; gap: var(--space-1); flex: 0 0 auto; }
    .icon-btn {
      position: relative; width: 40px; height: 40px; flex: 0 0 auto;
      display: inline-flex; align-items: center; justify-content: center;
      border: none; border-radius: 50%; background: transparent; color: var(--md-on-surface);
      cursor: pointer; transition: background-color var(--dur-fast) var(--ease-standard);
    }
    .icon-btn:hover { background: var(--md-surface-2); }
    .icon-btn:active { background: var(--md-divider); }
    .icon-btn:focus-visible { outline: 2px solid var(--md-primary); outline-offset: 2px; }
    .badge-count {
      position: absolute; top: 3px; right: 3px; min-width: 16px; height: 16px; padding: 0 4px;
      border-radius: 8px; background: var(--md-error); color: var(--md-on-error);
      font-size: 10px; font-weight: 500; line-height: 16px; text-align: center; letter-spacing: 0;
    }
    .badge-count[hidden] { display: none; }
    /* ===== 顶部更新横幅（常驻，只有用户点 × 才消失） ===== */
    #updateBanner { display: flex; flex-direction: column; gap: var(--space-2); margin-bottom: var(--space-6); }
    #updateBanner:empty { display: none; }
    .update-banner {
      display: flex; align-items: center; gap: var(--space-3); flex-wrap: wrap;
      background: var(--md-primary-light); color: var(--md-on-surface);
      border: 1px solid var(--md-primary-100); border-radius: var(--radius-card);
      padding: var(--space-3) var(--space-4); box-shadow: var(--elev-1);
    }
    .update-banner .ub-text { flex: 1 1 200px; min-width: 0; font-size: 0.875rem; }
    .update-banner .ub-repo { font-weight: 500; word-break: break-all; }
    .update-banner .ub-tag { font-family: var(--font-mono); font-size: 0.8125rem; color: var(--md-primary-700); }
    .update-banner .ub-time { font-size: 0.75rem; color: var(--md-on-surface-medium); margin-top: 2px; }
    .update-banner a.ub-link { color: var(--md-primary); font-size: 0.8125rem; font-weight: 500; }
    .update-banner.ub-more .ub-text { color: var(--md-on-surface-medium); }
    .ub-close {
      flex: 0 0 auto; width: 32px; height: 32px; border: none; border-radius: 50%;
      background: transparent; color: var(--md-on-surface-medium); cursor: pointer;
      display: inline-flex; align-items: center; justify-content: center;
      transition: background-color var(--dur-fast) var(--ease-standard), color var(--dur-fast) var(--ease-standard);
    }
    .ub-close:hover { background: var(--md-primary-100); color: var(--md-on-surface); }
    .ub-close:focus-visible { outline: 2px solid var(--md-primary); outline-offset: 1px; }
    .ub-text-btn { width: auto; height: 28px; padding: 0 10px; border-radius: var(--radius-button); font-size: 0.75rem; }
    /* ===== 右侧抽屉（通知中心 / 设置） ===== */
    .md-scrim {
      position: fixed; inset: 0; z-index: 300; background: rgba(0,0,0,0.42);
      opacity: 0; visibility: hidden;
      transition: opacity var(--dur-base) var(--ease-standard), visibility var(--dur-base) var(--ease-standard);
    }
    .md-scrim.open { opacity: 1; visibility: visible; }
    .md-drawer {
      position: fixed; top: 0; right: 0; bottom: 0; z-index: 301;
      width: min(420px, 100vw); max-width: 100vw;
      display: flex; flex-direction: column;
      background: var(--md-surface); color: var(--md-on-surface); box-shadow: var(--elev-16);
      transform: translateX(100%); visibility: hidden;
      transition: transform var(--dur-base) var(--ease-standard), visibility var(--dur-base) var(--ease-standard);
    }
    .md-drawer.open { transform: translateX(0); visibility: visible; }
    body.drawer-open { overflow: hidden; }
    .drawer-header {
      flex: 0 0 auto; display: flex; align-items: center; justify-content: space-between;
      height: 56px; padding: 0 var(--space-2) 0 var(--space-4);
      border-bottom: 1px solid var(--md-divider);
    }
    .drawer-header h2 { font-size: 16px; font-weight: 500; line-height: 24px; margin: 0; }
    .drawer-body { flex: 1 1 auto; overflow-y: auto; padding: var(--space-4); -webkit-overflow-scrolling: touch; }
    .drawer-pane { display: none; }
    .drawer-pane.active { display: block; }
    .drawer-group { margin-bottom: var(--space-6); }
    .drawer-group:last-child { margin-bottom: 0; }
    .drawer-group > h3 { font-size: 13px; font-weight: 500; line-height: 20px; margin: 0 0 var(--space-3); color: var(--md-primary); letter-spacing: 0.4px; }
    .drawer-group .form-group { margin-bottom: var(--space-3); }
    .drawer-group .form-group label { min-width: 110px; }
    .drawer-group .input { width: 100%; min-width: 0; }
    .drawer-group .btn { height: 36px; padding: 0 var(--space-4); font-size: 0.8125rem; }
    /* ===== 通知中心列表 ===== */
    .update-item {
      display: flex; align-items: flex-start; gap: var(--space-2);
      padding: var(--space-3); margin-bottom: var(--space-2);
      border: 1px solid var(--md-divider); border-radius: var(--radius-card); background: var(--md-surface);
      transition: background-color var(--dur-fast) var(--ease-standard);
    }
    .update-item:hover { background: var(--md-surface-2); }
    .update-item .ui-main { flex: 1 1 auto; min-width: 0; cursor: pointer; }
    .update-item .ui-repo { font-size: 0.875rem; font-weight: 500; word-break: break-all; }
    .update-item .ui-tag { font-family: var(--font-mono); font-weight: 400; }
    .update-item .ui-meta { font-size: 0.75rem; color: var(--md-on-surface-medium); margin-top: 2px; }
    .update-item.ignored { opacity: 0.65; }
    .update-item.ignored .ui-repo { text-decoration: line-through; }
    .empty-hint { padding: var(--space-4) 0; text-align: center; font-size: 0.8125rem; color: var(--md-on-surface-medium); }
  </style>
</head>
<body>
  <div id="authError" class="auth-error">⛔ 鉴权失败：请提供有效的 API Key</div>
  <header class="app-header">
    <div class="header-left">
      <h1>🔍 GitHub Release 监控</h1>
      <div class="subtitle" id="clock">北京时间 --:--:--</div>
    </div>
    <div class="header-actions">
      <button class="icon-btn" id="bellBtn" type="button" title="通知中心" aria-label="通知中心" onclick="openDrawer('notifications')">
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M18 8a6 6 0 1 0-12 0c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>
        <span class="badge-count" id="updateBadge" hidden>0</span>
      </button>
      <button class="icon-btn" id="gearBtn" type="button" title="设置" aria-label="设置" onclick="openDrawer('settings')">
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.6a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>
      </button>
    </div>
  </header>

  <!-- 站内通知中心：更新横幅（常驻显示，只有用户点击关闭才会消失） -->
  <div id="updateBanner"></div>

  <div class="card">
    <h2>➕ 添加新监控项目</h2>
    <div class="form-group">
      <input type="text" id="repoInput" class="input" placeholder="例如: vuejs/core" onkeydown="if(event.key==='Enter')addRepo()">
      <button class="btn btn-filled" onclick="addRepo()">确认添加</button>
    </div>
  </div>

  <div class="card">
    <h2>📦 批量导入 / 导出项目</h2>
    <p class="help-text">导入：选择 TXT，逐行识别 <code>owner/name</code>（可附加 <code>owner/name|备注</code>），自动跳过空白、注释（# 开头）、重复与非法行。导出：把所有监控项目按行输出为 TXT。</p>
    <div class="form-group">
      <input type="file" id="importFile" accept=".txt,text/plain" style="flex:1;min-width:200px;">
      <button class="btn btn-filled" onclick="importRepos()">📥 导入 TXT</button>
      <button class="btn btn-tonal" onclick="exportRepos()">📤 导出 TXT</button>
    </div>
    <div id="importResult" class="help-text"></div>
  </div>

  <div class="card">
    <h2>📡 检测状态</h2>
    <div id="stateInfo" class="info-panel">正在加载状态...</div>
  </div>

  <div class="card">
    <h2>📋 正在监控的项目 (<span id="repoCount">0</span>)</h2>
    <div class="table-wrapper">
      <table>
        <thead><tr><th>状态</th><th>项目路径</th><th>备注</th><th>通知链接</th><th style="width:80px">操作</th></tr></thead>
        <tbody id="repoTableBody"></tbody>
      </table>
    </div>
  </div>
  <!-- 右侧抽屉：通知中心 / 设置（Esc 或点击遮罩关闭） -->
  <div id="drawerScrim" class="md-scrim" onclick="closeDrawer()"></div>
  <aside id="drawerPanel" class="md-drawer" role="dialog" aria-modal="true" aria-labelledby="drawerTitle">
    <div class="drawer-header">
      <h2 id="drawerTitle">⚙️ 设置</h2>
      <button class="icon-btn" type="button" title="关闭" aria-label="关闭" onclick="closeDrawer()"><svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg></button>
    </div>
    <div class="drawer-body">
      <div id="drawerNotifications" class="drawer-pane">
        <div class="drawer-group">
          <h3>🔔 更新通知</h3>
          <div class="form-group" style="justify-content:space-between;gap:8px;">
            <span class="help-text" id="updateSummary" style="margin:0;">暂无更新</span>
            <button class="btn btn-outlined" id="clearAllUpdatesBtn" type="button">全部清除</button>
          </div>
          <div id="updateList"></div>
        </div>
        <div class="drawer-group">
          <button class="btn btn-tonal" id="toggleIgnoredBtn" type="button" style="width:100%;">查看已忽略 (0)</button>
          <div id="ignoredList" style="display:none;margin-top:12px;"></div>
        </div>
      </div>

      <div id="drawerSettings" class="drawer-pane">
        <div class="drawer-group">
          <h3>📢 站内通知中心</h3>
          <p class="help-text">开启后：检测到版本更新会直接在页面顶部常驻显示（不点关闭就不会消失），右上角铃铛显示未读数量，点条目可跳转到 Release 页面。关闭开关只隐藏入口，后台仍会继续记录更新事件。</p>
          <div class="form-group">
            <label for="inappEnabled">启用站内通知</label>
            <input type="checkbox" id="inappEnabled" checked>
          </div>
          <div class="form-group">
            <button class="btn btn-filled" type="button" onclick="saveInappChannel()">💾 保存</button>
            <span id="inappSaved" style="font-size:0.8125rem;color:var(--md-sys-color-primary);display:none;">✅ 已保存</span>
          </div>
        </div>

        <div class="drawer-group">
          <h3>🔔 通知渠道配置</h3>
          <p class="help-text">通知发送目的地。留空则回退使用 Cloudflare  secret 的 WEBHOOK_URL / WEBHOOK_AUTH_TOKEN。每个部署读自己的配置，互不干扰。</p>
          <div class="form-group">
            <label for="chEnabled">启用自定义通道</label>
            <input type="checkbox" id="chEnabled" checked>
          </div>
          <div class="form-group">
            <label for="chUrl">请求 URL</label>
            <input type="text" id="chUrl" class="input" placeholder="https://example.com/webhook">
          </div>
          <div class="form-group">
            <label for="chMethod">请求方法</label>
            <select id="chMethod" class="input">
              <option>POST</option><option>GET</option><option>PUT</option><option>PATCH</option>
            </select>
          </div>
          <div class="form-group">
            <label for="chToken">Authorization 令牌</label>
            <input type="text" id="chToken" class="input" placeholder="留空则不带 Authorization 头">
          </div>
          <div class="form-group">
            <label for="chBody">请求体模板</label>
          </div>
          <textarea id="chBody" spellcheck="false" placeholder='{"title":"{title}","content":"{content}"}' style="width:100%;min-height:90px;font-family:monospace;font-size:0.8rem;"></textarea>
          <p class="help-text">可用变量：<code>{title}</code> <code>{content}</code> <code>{repo_name}</code> <code>{url}</code> <code>{tag}</code> <code>{message}</code> 等（取自上方通知内容模板的解析结果）。</p>
          <div class="form-group" style="margin-top:12px;">
            <button class="btn btn-filled" type="button" onclick="saveNotifyChannel()">💾 保存通道</button>
            <span id="chSaved" style="font-size:0.8125rem;color:var(--md-sys-color-primary);display:none;">✅ 已保存</span>
          </div>
        </div>

        <div class="drawer-group">
          <h3>⚙️ 检测节奏与免打扰</h3>
          <div class="form-group">
            <label for="repoInterval">仓库检查间隔（分钟）</label>
            <input type="number" id="repoInterval" class="input" min="5" max="60" value="5">
            <span style="font-size:0.75rem;color:var(--md-sys-color-on-surface-variant);">每仓库等待时间</span>
          </div>
          <div class="form-group">
            <label for="cycleInterval">检测周期间隔（小时）</label>
            <input type="number" id="cycleInterval" class="input" min="1" max="48" value="8">
            <span style="font-size:0.75rem;color:var(--md-sys-color-on-surface-variant);">两轮检测之间等待</span>
          </div>
          <hr style="border:none;border-top:1px solid var(--md-sys-color-outline-variant);margin:8px 0 16px;">
          <p class="help-text">🔕 免打扰模式：在指定时段（北京时间 UTC+8）内不发送任何通知；若此间有版本更新，将保留并在免打扰结束后自动补发。</p>
          <div class="form-group">
            <label for="dndEnabled">启用免打扰</label>
            <input type="checkbox" id="dndEnabled">
            <span id="dndStatus" style="font-size:0.8rem;color:var(--md-sys-color-on-surface-variant);"></span>
          </div>
          <div class="form-group">
            <label for="dndStart">开始时间 (北京时间)</label>
            <input type="time" id="dndStart" class="input" value="23:00">
          </div>
          <div class="form-group">
            <label for="dndEnd">结束时间 (北京时间)</label>
            <input type="time" id="dndEnd" class="input" value="08:00">
            <span style="font-size:0.75rem;color:var(--md-sys-color-on-surface-variant);">结束≤开始表示跨午夜</span>
          </div>
          <div class="form-group">
            <button class="btn btn-filled" onclick="saveSettings()">💾 保存设置</button>
            <span id="settingsSaved" style="color:var(--md-sys-color-primary);display:none;">✅ 已保存</span>
          </div>
        </div>

        <div class="drawer-group">
          <h3>📝 通知内容配置</h3>
          <p class="help-text">
            可用变量：<b>update</b>：<code>{repo}</code> <code>{repo_name}</code> <code>{url}</code> <code>{repo_url}</code> <code>{tag}</code>
            &nbsp;&nbsp;<b>alert</b>：<code>{repo}</code> <code>{message}</code>
          </p>
          <textarea id="notificationTemplate" spellcheck="false" placeholder="JSON 模板内容..."></textarea>
          <div class="form-group" style="margin-top:12px;">
            <button class="btn btn-filled" onclick="saveNotificationConfig()">💾 保存模板</button>
            <span id="notifSaved" style="color:var(--md-sys-color-primary);display:none;">✅ 已保存</span>
          </div>
        </div>

        <div class="drawer-group">
          <h3>🧪 手动操作</h3>
          <div style="display:flex;gap:12px;flex-wrap:wrap;">
            <button class="btn btn-tonal" id="testBtn" type="button" onclick="runTest()">🎯 立即测试（随机一个仓库）</button>
            <button class="btn btn-outlined" type="button" onclick="triggerCycle()">🔄 触发新一轮检测</button>
          </div>
          <div id="loadingText" style="display:none;margin-top:12px;font-size:0.8125rem;color:var(--md-sys-color-on-surface-variant);">⏳ 正在执行，请稍候...</div>
          <pre id="resultBlock" class="result-block">// 操作结果显示在这里</pre>
        </div>
      </div>
    </div>
  </aside>

  <button class="md-fab" title="添加监控仓库" aria-label="添加监控仓库" onclick="document.getElementById('repoInput').focus()"><svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg></button>

  <script>
    const params = new URLSearchParams(window.location.search);
    let API_KEY = sessionStorage.getItem('api_key') || '';
    if (params.get('key')) { API_KEY = params.get('key'); sessionStorage.setItem('api_key', API_KEY); window.history.replaceState({}, document.title, window.location.origin + window.location.pathname); }
    function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
    function escAttr(s) { return String(s).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/'/g,'&#39;'); }
    // 将 UTC ISO 时间字符串格式化为北京时间（UTC+8）显示；withZone=false 时只返回 "YYYY-MM-DD HH:MM:SS"
    function fmtBJ(iso, withZone = true) {
      const d = (typeof iso === 'string') ? new Date(iso) : iso;
      if (!d || isNaN(d.getTime())) return '';
      const b = new Date(d.getTime() + 8 * 3600 * 1000);
      const p = n => String(n).padStart(2, '0');
      const s = b.getUTCFullYear() + '-' + p(b.getUTCMonth() + 1) + '-' + p(b.getUTCDate()) + ' ' + p(b.getUTCHours()) + ':' + p(b.getUTCMinutes()) + ':' + p(b.getUTCSeconds());
      return withZone ? s + ' (北京时间)' : s;
    }
    function tickClock() {
      const el = document.getElementById('clock');
      if (el) el.textContent = '北京时间 ' + fmtBJ(new Date(), false);
    }

    async function apiFetch(url, options = {}) {
      const headers = options.headers || {};
      if (API_KEY) headers['X-API-Key'] = API_KEY;
      const res = await fetch(url, { ...options, headers });
      if (res.status === 403) { document.getElementById('authError').style.display = 'block'; throw new Error('鉴权失败'); }
      if (res.status === 503) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.error || '数据库不可用');
      }
      if (res.status === 500) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.error || '服务端错误');
      }
      return res;
    }

    document.addEventListener('DOMContentLoaded', () => {
      if (!API_KEY) document.getElementById('authError').style.display = 'block';
      cacheDomRefs();
      tickClock(); setInterval(tickClock, 1000);
      document.getElementById('repoTableBody').addEventListener('click', (e) => {
        const btn = e.target.closest('.delete-repo-btn');
        if (btn) { const repo = btn.dataset.repo; if (repo) deleteRepo(repo); }
      });
      document.getElementById('repoTableBody').addEventListener('change', (e) => {
        const input = e.target.closest('.note-input');
        if (input) saveNote(input.dataset.repo, input.value);
      });
      bindUpdateEvents();
      loadSettings(); loadNotificationConfig(); loadNotifyChannel(); loadRepos();
      // 站内通知中心：首次加载即拉取，之后每 60 秒轮询一次（横幅常驻，不会自动消失）
      loadInappChannel(); loadUpdates();
      setInterval(loadUpdates, 60000);
    });

    async function loadSettings() {
      try {
        const res = await apiFetch('/api/get-settings');
        const { settings, state } = await res.json();
        document.getElementById('repoInterval').value = settings.repoIntervalMinutes;
        document.getElementById('cycleInterval').value = settings.cycleIntervalHours;
        const dnd = settings.dnd || { enabled: false, start: "23:00", end: "08:00" };
        document.getElementById('dndEnabled').checked = !!dnd.enabled;
        document.getElementById('dndStart').value = dnd.start || "23:00";
        document.getElementById('dndEnd').value = dnd.end || "08:00";
        updateDndStatus(settings);
        renderState(state, settings);
      } catch (e) {
        document.getElementById('stateInfo').innerHTML = '❌ 加载失败 — ' + esc(e.message || '请检查 D1 绑定与 API Key');
      }
    }
    function isDndActiveClient(dnd) {
      if (!dnd || !dnd.enabled) return false;
      const parse = (s) => { const m=/^(\\d{1,2}):(\\d{2})$/.exec(s||""); if(!m) return null; const h=+m[1],mi=+m[2]; if(h>23||mi>59) return null; return h*60+mi; };
      const s = parse(dnd.start), e = parse(dnd.end);
      if (s===null||e===null||s===e) return false;
      const d = new Date();
      const cur = (d.getUTCHours()*60 + d.getUTCMinutes() + 8*60) % (24*60);
      if (s<e) return cur>=s && cur<e;
      return cur>=s || cur<e;
    }
    function updateDndStatus(settings) {
      const el = document.getElementById('dndStatus');
      if (!el) return;
      const dnd = settings.dnd;
      if (!dnd || !dnd.enabled) { el.textContent = '（已关闭）'; el.style.color = 'var(--md-sys-color-on-surface-variant)'; return; }
      if (dnd.start === dnd.end) { el.textContent = '⚠️ 开始=结束，免打扰未生效（请设为不同时间）'; el.style.color = 'var(--md-sys-color-error)'; return; }
      if (isDndActiveClient(dnd)) { el.textContent = '🔕 当前处于免打扰时段'; el.style.color = 'var(--md-sys-color-error)'; }
      else { el.textContent = '🟢 当前可通知（北京时间 '+dnd.start+'–'+dnd.end+' 免打扰）'; el.style.color = 'var(--md-sys-color-primary)'; }
    }
    function renderState(state, settings) {
      const el = document.getElementById('stateInfo');
      if (!state || !state.phase || !state.cycleStartTime && !state.cycleEndTime) {
        el.innerHTML = '⚠️ 调度服务未就绪，请检查 D1 配置';
        return;
      }
      let total = parseInt(state.totalRepos,10)||0;
      if (total===0) total = parseInt(document.getElementById('repoCount').textContent,10)||0;
      let html = '';
      if (state.phase==='checking') {
        let nextIn = settings.repoIntervalMinutes;
        if (state.lastRepoCheckTime) {
          const elapsed = (Date.now() - new Date(state.lastRepoCheckTime).getTime())/60000;
          nextIn = Math.max(0, Math.ceil(settings.repoIntervalMinutes - elapsed));
        }
        html = '🟢 <b>检测中</b> — 进度: '+state.currentIndex+'/'+total+' | 下一个仓库约 '+nextIn+' 分钟后检查';
        if (state.lastRepoCheckTime) html += '<br>上次检查: '+esc(fmtBJ(state.lastRepoCheckTime));
      } else {
        const cycleMs = settings.cycleIntervalHours*3600000;
        const waitSince = state.cycleEndTime ? new Date(state.cycleEndTime) : (state.cycleStartTime ? new Date(state.cycleStartTime) : new Date());
        const next = new Date(waitSince.getTime()+cycleMs);
        html = '💤 <b>等待中</b> — 预计下次: '+esc(fmtBJ(next.toISOString()));
        if (state.cycleStartTime) html += '<br>本轮开始: '+esc(fmtBJ(state.cycleStartTime));
        if (state.cycleEndTime) html += '，结束: '+esc(fmtBJ(state.cycleEndTime));
      }
      if (settings.dnd && settings.dnd.enabled && isDndActiveClient(settings.dnd)) {
        html += '<br>🔕 免打扰时段中，更新将延后通知';
      }
      el.innerHTML = html;
    }

    async function saveSettings() {
      const repoIntervalMinutes = parseInt(document.getElementById('repoInterval').value,10);
      const cycleIntervalHours = parseInt(document.getElementById('cycleInterval').value,10);
      const dnd = {
        enabled: document.getElementById('dndEnabled').checked,
        start: document.getElementById('dndStart').value || "23:00",
        end: document.getElementById('dndEnd').value || "08:00"
      };
      try {
        const res = await apiFetch('/api/save-settings', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ repoIntervalMinutes, cycleIntervalHours, dnd }) });
        const data = await res.json();
        if (data.success) { document.getElementById('settingsSaved').style.display = 'inline'; setTimeout(() => document.getElementById('settingsSaved').style.display = 'none', 2600); loadSettings(); }
        else alert('错误: ' + data.error);
      } catch (e) { alert('保存失败: ' + e.message); }
    }

    async function loadNotificationConfig() {
      try { const res = await apiFetch('/api/get-notification-config'); document.getElementById('notificationTemplate').value = JSON.stringify(await res.json(), null, 2); } catch (e) {}
    }
    async function saveNotificationConfig() {
      const textarea = document.getElementById('notificationTemplate');
      let config; try { config = JSON.parse(textarea.value); } catch (e) { alert('JSON 格式无效'); return; }
      if (!config.update || !config.alert) { alert('配置必须包含 update 和 alert 对象'); return; }
      try {
        const res = await apiFetch('/api/save-notification-config', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(config) });
        const data = await res.json();
        if (data.success) { document.getElementById('notifSaved').style.display = 'inline'; setTimeout(() => document.getElementById('notifSaved').style.display = 'none', 2600); }
        else alert('保存失败: ' + data.error);
      } catch (e) { alert('保存失败: ' + e.message); }
    }

    async function loadNotifyChannel() {
      try {
        const res = await apiFetch('/api/get-notify-channel');
        const data = await res.json();
        const ch = data.channel;
        if (!ch) return;
        document.getElementById('chEnabled').checked = ch.enabled !== false;
        document.getElementById('chUrl').value = ch.url || '';
        const m = document.getElementById('chMethod');
        if (ch.method) { for (const o of m.options) { if (o.value === ch.method) { o.selected = true; break; } } }
        document.getElementById('chToken').value = (ch.headers && ch.headers['Authorization'] && ch.headers['Authorization'] !== '***') ? ch.headers['Authorization'] : '';
        document.getElementById('chBody').value = ch.bodyTemplate || '';
      } catch (e) { /* 忽略 */ }
    }
    async function saveNotifyChannel() {
      const enabled = document.getElementById('chEnabled').checked;
      const url = document.getElementById('chUrl').value.trim();
      const method = document.getElementById('chMethod').value;
      const token = document.getElementById('chToken').value.trim();
      const bodyTemplate = document.getElementById('chBody').value;
      const headers = { 'Content-Type': 'application/json' };
      if (token) headers['Authorization'] = token;
      const payload = { enabled, url, method, headers, bodyTemplate };
      try {
        const res = await apiFetch('/api/save-notify-channel', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
        const data = await res.json();
        if (data.success) { document.getElementById('chSaved').style.display = 'inline'; setTimeout(() => { document.getElementById('chSaved').style.display = 'none'; }, 2000); }
        else if (data.error) alert(data.error);
      } catch (e) { alert('保存失败: ' + e.message); }
    }

    // ==================== 右侧抽屉（通知中心 / 设置） ====================
    function openDrawer(name) {
      const isNotif = name === 'notifications';
      document.getElementById('drawerNotifications').classList.toggle('active', isNotif);
      document.getElementById('drawerSettings').classList.toggle('active', !isNotif);
      document.getElementById('drawerTitle').textContent = isNotif ? '🔔 通知中心' : '⚙️ 设置';
      document.getElementById('drawerScrim').classList.add('open');
      document.getElementById('drawerPanel').classList.add('open');
      document.body.classList.add('drawer-open');
      if (isNotif) renderUpdates();
    }
    function closeDrawer() {
      document.getElementById('drawerScrim').classList.remove('open');
      document.getElementById('drawerPanel').classList.remove('open');
      document.body.classList.remove('drawer-open');
    }
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeDrawer(); });

    // ==================== 站内通知中心 ====================
    const DISMISS_KEY = 'grm_dismissed_updates';
    const DISMISS_MAX = 2000;
    let inappEnabled = true;      // 站内渠道开关（只影响前端展示，后端始终记录）
    let updatesCache = [];        // /api/get-updates 的原始列表
    let showIgnored = false;      // 是否展开「已忽略」

    // DOM 引用缓存：render 系列函数高频调用，避免每次 getElementById（DOMContentLoaded 时一次性获取）
    let elUpdateBanner = null, elUpdateList = null, elIgnoredList = null,
        elToggleIgnoredBtn = null, elUpdateSummary = null, elUpdateBadge = null, elBellBtn = null;
    function cacheDomRefs() {
      elUpdateBanner = document.getElementById('updateBanner');
      elUpdateList = document.getElementById('updateList');
      elIgnoredList = document.getElementById('ignoredList');
      elToggleIgnoredBtn = document.getElementById('toggleIgnoredBtn');
      elUpdateSummary = document.getElementById('updateSummary');
      elUpdateBadge = document.getElementById('updateBadge');
      elBellBtn = document.getElementById('bellBtn');
    }

    // 只接受 http(s) 绝对地址，其余一律不渲染成链接
    function safeUrl(u) {
      if (typeof u !== 'string') return '';
      return (u.indexOf('http://') === 0 || u.indexOf('https://') === 0) ? u : '';
    }
    function openRelease(url) {
      const w = window.open(url, '_blank');
      if (w) w.opener = null;
    }
    function readDismissed() {
      try {
        const arr = JSON.parse(localStorage.getItem(DISMISS_KEY) || '[]');
        return Array.isArray(arr) ? arr.map(Number).filter(n => !isNaN(n)) : [];
      } catch (e) { return []; }
    }
    function writeDismissed(arr) {
      try {
        let kept = arr;
        // 裁剪时只保留仍在 updatesCache 中出现的 id，避免丢弃最早的已关闭记录导致它们「复活」；
        // updatesCache 为空（尚未拉取完成）时保持原样，避免误删。
        if (updatesCache.length) {
          const live = updatesCache.map(u => Number(u.id));
          kept = kept.filter(id => live.indexOf(Number(id)) !== -1);
        }
        localStorage.setItem(DISMISS_KEY, JSON.stringify(kept.slice(-DISMISS_MAX)));
      } catch (e) { /* 忽略 */ }
    }
    function dismissUpdate(id) {
      const arr = readDismissed();
      if (arr.indexOf(Number(id)) === -1) arr.push(Number(id));
      writeDismissed(arr);
      renderUpdates();
    }
    function restoreUpdate(id) {
      writeDismissed(readDismissed().filter(n => n !== Number(id)));
      renderUpdates();
    }
    function clearAllUpdates() {
      const merged = readDismissed().concat(updatesCache.map(u => Number(u.id)));
      writeDismissed(merged.filter((v, i, a) => a.indexOf(v) === i));
      renderUpdates();
    }
    function splitUpdates() {
      const dismissed = readDismissed();
      const active = [], ignored = [];
      for (const u of updatesCache) {
        (dismissed.indexOf(Number(u.id)) === -1 ? active : ignored).push(u);
      }
      return { active, ignored };
    }

    // 数据签名：id + tag + url + detected_at 拼接，用于判断 /api/get-updates 数据是否真的变化
    function updatesSignature(list) {
      return list.length + '|' + list.map(function (u) { return u.id + ':' + (u.repo || '') + ':' + u.tag + ':' + (u.url || '') + ':' + (u.detected_at || ''); }).join(',');
    }
    let updatesInFlight = false;  // 轮询防重入：上一次 /api/get-updates 未结束时直接跳过
    async function loadUpdates() {
      if (updatesInFlight) return;
      updatesInFlight = true;
      try {
        const res = await apiFetch('/api/get-updates');
        const data = await res.json();
        const fresh = Array.isArray(data.updates) ? data.updates : [];
        // 性能：签名没变（数据无变化）就跳过 renderUpdates，避免每 60s 全量重建横幅/列表 DOM
        if (updatesSignature(fresh) !== updatesSignature(updatesCache)) {
          updatesCache = fresh;
          renderUpdates();
        }
      } catch (e) { /* 瞬时错误保留旧数据并跳过本次渲染，避免横幅/列表闪烁消失 */ return; }
      finally { updatesInFlight = false; }
    }

    async function loadInappChannel() {
      try {
        const res = await apiFetch('/api/get-inapp-channel');
        const data = await res.json();
        inappEnabled = !data.channel || data.channel.enabled !== false;
      } catch (e) { inappEnabled = true; }
      const cb = document.getElementById('inappEnabled');
      if (cb) cb.checked = inappEnabled;
      renderUpdates();
    }

    async function saveInappChannel() {
      const cb = document.getElementById('inappEnabled');
      const enabled = cb ? cb.checked : true;
      try {
        const res = await apiFetch('/api/save-inapp-channel', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ enabled }) });
        const data = await res.json();
        if (data.success) {
          inappEnabled = enabled;
          const s = document.getElementById('inappSaved');
          if (s) { s.style.display = 'inline'; setTimeout(() => { s.style.display = 'none'; }, 2000); }
          renderUpdates();
        } else if (data.error) alert(data.error);
      } catch (e) { alert('保存失败: ' + e.message); }
    }

    // 统一渲染：顶部横幅 + 通知列表 + 铃铛未读数
    function renderUpdates() {
      const parts = splitUpdates();
      renderBanners(inappEnabled ? parts.active : []);
      renderUpdateList(inappEnabled ? parts.active : [], inappEnabled ? parts.ignored : []);
      const badge = elUpdateBadge;
      if (badge) {
        if (inappEnabled && parts.active.length) {
          badge.textContent = parts.active.length > 99 ? '99+' : String(parts.active.length);
          badge.hidden = false;
        } else {
          badge.hidden = true;
        }
      }
      const bell = elBellBtn;
      if (bell) bell.style.display = inappEnabled ? '' : 'none';
    }

    const CLOSE_SVG = '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>';

    // 顶部横幅：常驻显示，不设任何自动隐藏定时器；最多只展示最新 BANNER_MAX 条，
    // 避免极端情况下堆叠上百条把主页内容挤走，多余的折叠成一条「查看全部」入口。
    const BANNER_MAX = 5;
    function renderBanners(list) {
      const box = elUpdateBanner;
      if (!box) return;
      if (!list.length) { box.innerHTML = ''; return; }
      const shown = list.slice(0, BANNER_MAX);
      let html = shown.map(u => {
        const url = safeUrl(u.url);
        const link = url
          ? '<a class="ub-link" href="' + escAttr(url) + '" target="_blank" rel="noopener noreferrer">查看 Release ↗</a>'
          : '<span class="ub-link" style="color:var(--md-sys-color-on-surface-variant);">无可用链接</span>';
        return '<div class="update-banner" data-id="' + escAttr(u.id) + '">' +
            '<div class="ub-text">' +
              '<div><span class="ub-repo">🚀 ' + esc(u.repo) + '</span> <span class="ub-tag">' + esc(u.tag) + '</span></div>' +
              '<div class="ub-time">检测于 ' + esc(fmtBJ(u.detected_at)) + '</div>' +
            '</div>' + link +
            '<button class="ub-close" type="button" data-action="dismiss" data-id="' + escAttr(u.id) + '" title="关闭这条通知" aria-label="关闭这条通知">' + CLOSE_SVG + '</button>' +
          '</div>';
      }).join('');
      const rest = list.length - shown.length;
      if (rest > 0) {
        html += '<div class="update-banner ub-more">' +
            '<div class="ub-text">还有 ' + rest + ' 条更新</div>' +
            '<button class="ub-close ub-text-btn" type="button" data-action="drawer" data-drawer="notifications">查看全部</button>' +
          '</div>';
      }
      box.innerHTML = html;
    }

    function updateItemHtml(u, isIgnored) {
      const url = safeUrl(u.url);
      const urlAttr = url ? ' data-url="' + escAttr(url) + '"' : '';
      const actionBtn = isIgnored
        ? '<button class="ub-close ub-text-btn" type="button" data-action="restore" data-id="' + escAttr(u.id) + '">恢复</button>'
        : '<button class="ub-close" type="button" data-action="dismiss" data-id="' + escAttr(u.id) + '" title="关闭这条通知" aria-label="关闭这条通知">' + CLOSE_SVG + '</button>';
      const tips = (isIgnored ? ' · 已忽略' : '') + (url ? ' · 点击打开 Release' : '');
      return '<div class="update-item' + (isIgnored ? ' ignored' : '') + '" data-id="' + escAttr(u.id) + '">' +
          '<div class="ui-main" data-action="open"' + urlAttr + '>' +
            '<div class="ui-repo">' + esc(u.repo) + ' <span class="ui-tag">' + esc(u.tag) + '</span></div>' +
            '<div class="ui-meta">' + esc(fmtBJ(u.detected_at)) + tips + '</div>' +
          '</div>' + actionBtn +
        '</div>';
    }

    function renderUpdateList(active, ignored) {
      const box = elUpdateList;
      const igBox = elIgnoredList;
      const toggleBtn = elToggleIgnoredBtn;
      const summary = elUpdateSummary;
      if (summary) summary.textContent = active.length ? ('共 ' + active.length + ' 条未关闭的更新') : '暂无未关闭的更新';
      if (toggleBtn) toggleBtn.textContent = '查看已忽略 (' + ignored.length + ')';
      if (box) box.innerHTML = active.length ? active.map(u => updateItemHtml(u, false)).join('') : '<div class="empty-hint">暂无更新通知</div>';
      if (igBox) {
        igBox.style.display = showIgnored ? 'block' : 'none';
        igBox.innerHTML = showIgnored
          ? (ignored.length ? ignored.map(u => updateItemHtml(u, true)).join('') : '<div class="empty-hint">没有已忽略的更新</div>')
          : '';
      }
    }

    // 事件委托：不使用 onclick 内联插值，全部走 data-action / data-id
    function bindUpdateEvents() {
      const handler = (e) => {
        const el = e.target.closest('[data-action]');
        if (!el) return;
        const id = el.getAttribute('data-id');
        const action = el.getAttribute('data-action');
        if (action === 'dismiss' && id) dismissUpdate(id);
        else if (action === 'restore' && id) restoreUpdate(id);
        else if (action === 'drawer') {
          const d = el.getAttribute('data-drawer');
          if (d) openDrawer(d);
        }
        else if (action === 'open') {
          const url = safeUrl(el.getAttribute('data-url'));
          if (url) openRelease(url);
        }
      };
      document.getElementById('updateBanner').addEventListener('click', handler);
      document.getElementById('drawerNotifications').addEventListener('click', handler);
      document.getElementById('clearAllUpdatesBtn').addEventListener('click', clearAllUpdates);
      document.getElementById('toggleIgnoredBtn').addEventListener('click', () => { showIgnored = !showIgnored; renderUpdates(); });
    }

    async function triggerCycle() {
      try {
        const res = await apiFetch('/api/trigger-cycle', { method:'POST' });
        document.getElementById('resultBlock').textContent = JSON.stringify(await res.json(), null, 2);
        await loadSettings();
      } catch (e) {
        document.getElementById('resultBlock').textContent = '请求失败: ' + e.message;
      }
    }

    async function loadRepos() {
      try { const res = await apiFetch('/api/get-repos'); renderTable(await res.json()); } catch (e) {}
    }
    function renderTable(repos) {
      document.getElementById('repoCount').textContent = repos.length;
      const tbody = document.getElementById('repoTableBody');
      if (!repos.length) { tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;padding:24px;color:var(--md-sys-color-on-surface-variant);">暂无监控项目</td></tr>'; return; }
      tbody.innerHTML = repos.map(item => {
        let badgeClass = 'badge-ok', badgeText = '正常';
        if (item.health==='dead') { badgeClass = 'badge-dead'; badgeText = '失效'; }
        else if (item.health==='warning') { badgeClass = 'badge-warn'; badgeText = '异常'; }
        else if (item.health==='recovered') { badgeClass = 'badge-recov'; badgeText = '观察中'; }
        const tip = (item.lastError||'') + (item.reason ? ' | 原因：'+item.reason : '') + (item.judgeReason ? ' | 判定：'+item.judgeReason : '');
        return '<tr><td><span class="badge '+badgeClass+'" title="'+escAttr(tip)+'">'+badgeText+'</span></td><td>'+esc(item.repo)+'</td><td><input class="note-input" data-repo="'+escAttr(item.repo)+'" value="'+escAttr(item.note||'')+'" placeholder="备注..."></td><td><code style="background:var(--md-sys-color-surface-variant);padding:2px 6px;border-radius:4px;">'+esc(item.custom_url)+'</code></td><td><button class="btn btn-error delete-repo-btn" data-repo="'+escAttr(item.repo)+'" style="height:32px;padding:0 12px;font-size:0.7rem;">删除</button></td></tr>';
      }).join('');
    }

    async function addRepo() {
      const input = document.getElementById('repoInput'); const repo = input.value.trim(); if (!repo) return;
      try {
        const res = await apiFetch('/api/add-repo', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ repo }) });
        const data = await res.json();
        if (data.success) { input.value = ''; renderTable(data.repos); loadSettings(); }
        else alert('错误: ' + data.error);
      } catch (e) { alert('请求失败: ' + e.message); }
    }

    async function deleteRepo(repo) {
      if (!confirm('确定取消监控 ' + repo + ' 吗？')) return;
      try {
        const res = await apiFetch('/api/delete-repo', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ repo }) });
        const data = await res.json();
        if (data.success) { renderTable(data.repos); loadSettings(); }
        else alert('错误: ' + data.error);
      } catch (e) { alert('请求失败: ' + e.message); }
    }

    async function importRepos() {
      const fileInput = document.getElementById('importFile');
      const resultEl = document.getElementById('importResult');
      const file = fileInput.files && fileInput.files[0];
      if (!file) { resultEl.textContent = '⚠️ 请先选择一个 .txt 文件'; resultEl.style.color = 'var(--md-sys-color-error)'; return; }
      resultEl.textContent = '⏳ 正在导入...'; resultEl.style.color = 'var(--md-sys-color-on-surface-variant)';
      try {
        const content = await file.text();
        const res = await apiFetch('/api/import-repos', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ content }) });
        const data = await res.json();
        if (data.success) {
          let msg = '✅ 导入完成：新增 ' + data.imported + ' 个，跳过重复 ' + data.skipped + ' 个，非法 ' + data.invalid + ' 行';
          if (data.invalidLines && data.invalidLines.length) msg += '\\n⚠️ 非法行示例：' + data.invalidLines.slice(0,3).join(' | ');
          resultEl.textContent = msg; resultEl.style.color = 'var(--md-sys-color-primary)';
          renderTable(data.repos); loadSettings();
          fileInput.value = '';
        } else { resultEl.textContent = '❌ ' + (data.error || '导入失败'); resultEl.style.color = 'var(--md-sys-color-error)'; }
      } catch (e) { resultEl.textContent = '请求失败: ' + e.message; resultEl.style.color = 'var(--md-sys-color-error)'; }
    }

    async function exportRepos() {
      try {
        const res = await apiFetch('/api/export-repos');
        const txt = await res.text();
        const blob = new Blob([txt], { type: 'text/plain;charset=utf-8' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url; a.download = 'repos_export.txt';
        document.body.appendChild(a); a.click(); a.remove();
        URL.revokeObjectURL(url);
      } catch (e) { alert('导出失败: ' + e.message); }
    }

    async function saveNote(repo, note) {
      try {
        await apiFetch('/api/update-note', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ repo, note }) });
      } catch (e) { alert('备注保存失败: ' + e.message); }
    }

    async function runTest() {
      const btn = document.getElementById('testBtn'), loading = document.getElementById('loadingText'), resultBlock = document.getElementById('resultBlock');
      btn.disabled = true; loading.style.display = 'block'; resultBlock.textContent = '// 随机选取一个仓库，跑完整「检测 + 发送通知」逻辑中...';
      try {
        const res = await apiFetch('/api/test');
        if (res.status === 429) {
          resultBlock.textContent = '请求过于频繁，请 10 秒后再试';
        } else {
          const data = await res.json();
          const sep = String.fromCharCode(10);
          const lines = [];
          lines.push('随机选中仓库: ' + (data.picked || '未知'));
          lines.push('监控仓库总数: ' + (data.total || 0));
          if (data.results && data.results[0]) {
            const r = data.results[0];
            lines.push('检测成功: ' + (r.success ? '是' : '否'));
            lines.push('通知已发送: ' + (r.push_ok ? '是' : '否'));
            if (r.push_ok === false) lines.push('提示: 通知发送失败，请检查 WEBHOOK_URL / WEBHOOK_AUTH_TOKEN 配置');
          }
          lines.push('');
          lines.push(JSON.stringify(data, null, 2));
          resultBlock.textContent = lines.join(sep);
        }
      }
      catch (e) { resultBlock.textContent = '请求失败: ' + e.message; }
      finally {
        btn.disabled = false; loading.style.display = 'none';
        // 测试期间抽屉若被关掉，重新打开设置抽屉，保证用户能看到结果
        const panel = document.getElementById('drawerPanel');
        if (panel && !panel.classList.contains('open')) openDrawer('settings');
        // 手动测试会在后端写一条站内通知，立即拉取刷新横幅/铃铛/通知列表，不等 60s 轮询
        loadUpdates();
      }
    }
  </script>
</body>
</html>`;