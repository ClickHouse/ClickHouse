#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs node (present in the stateless-test image, not in the fasttest one)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for the Web UI query-parameter bookkeeping of the "history on run, not on
# keypress" contract in play.html, driven through the REAL parameter pipeline: the real embedded
# WASM lexer (`tokenize`), the real `updateQueryParams` input rebuild (including the trusted
# `param_*` input listener it installs and `syncParamsAfterRebuild`), and the real launch-time
# binding the run path uses (`resolveRunParams` + `extractRunParamNames` + `pickRunParams`),
# feeding the real `saveHistory`/`writeHistoryEntry`/`tabReflectsRun`. Pinned scenarios:
#   - a query edit that REMOVES a placeholder folds the rebuilt inputs into `tab.params`
#     (`syncParamsAfterRebuild`), so a clean rerun of the new text keeps `run=1` and neither the
#     entry nor the URL resurrects the removed `param_*` binding;
#   - REORDERING placeholders is not a change of bindings: the launch snapshot follows the text's
#     placeholder order while `tab.params` may keep the previous order, and the order-insensitive
#     comparison (`sameParamValues`) keeps `run=1` on a clean run and keeps the tab reading as
#     clean (`tabReflectsRun`) for later editor-only history writers;
#   - a placeholder-removing edit made while a run is IN FLIGHT: the completed run must not leak
#     the stale binding under the newer draft — whichever side wins the race. When the rebuild
#     lands first, `tab.params` already follows the draft; when the COMPLETION writes first, its
#     own entry must already be clean (`saveHistory` prunes the map by a conservative textual
#     placeholder scan of the draft, `queryMentionsParam`, so the stale `param_*` never reaches
#     the URL even transiently), while a placeholder that SURVIVES the edit keeps its binding;
#     the landing rebuild then re-folds the inputs authoritatively
#     (`syncParamsAfterRebuild` -> `refreshCurrentHistoryEntry`).
# The harness extracts the real functions from the served /play page and drives them under node
# with stub DOM/history objects; the parameter inputs are real enough for `updateQueryParams` to
# build (create/remove elements, value carry-over, the trusted-edit listener).

html="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_play.html"
harness="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_play_param_harness.js"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/play" > "$html"

cat > "$harness" << 'EOF'
const fs = require('fs');
const vm = require('vm');

const html = fs.readFileSync(process.argv[2], 'utf8');
const script_match = html.match(/<script type="text\/javascript">([\s\S]*?)<\/script>/);
if (!script_match) throw new Error('cannot find the main script in play.html');
const src = script_match[1];

/// Extract a top-level definition: from its header line to the first line that is a
/// closing brace at column 0 (the file's uniform style for top-level functions).
/// A rename or restructure makes this throw, failing the test loudly.
function extractTopLevel(header_re, name)
{
    const lines = src.split('\n');
    const start = lines.findIndex(line => header_re.test(line));
    if (start === -1) throw new Error('play.html no longer defines ' + name + ' at top level; update this test');
    for (let i = start + 1; i < lines.length; i++)
        if (/^};?\s*$/.test(lines[i])) return lines.slice(start, i + 1).join('\n');
    throw new Error('cannot find the end of ' + name);
}

/// The real code under test: the tab/history bookkeeping AND the full parameter pipeline —
/// the embedded WASM lexer, the input rebuild and the launch-time binding — taken verbatim
/// from the page. Only rendering and persistence I/O are stubbed below.
const FUNCS = ['toBase64', 'fromBase64', 'nextDefaultTitle', 'uniqueTitle', 'tabRuntimeDefaults',
    'makeTab', 'nextRunId', 'getActiveTab', 'captureActiveTab', 'buildHistoryParams',
    'writeHistoryEntry', 'sameParamValues', 'queryMentionsParam', 'tabReflectsRun', 'liveDivergedFromRun',
    'effectiveDatabase', 'sameServerAddress', 'effectiveConnectionUser',
    'stampSelectedDatabaseConnection', 'refreshCurrentHistoryEntry', 'saveHistory', 'syncHistory',
    'markBootstrapDirty', 'scheduleSave', 'persist', 'restoreEditor',
    'extractQueryParams', 'getParamValues', 'setParamValues', 'pickRunParams', 'resolveRunParams',
    'extractRunParamNames', 'syncParamsAfterRebuild', 'updateQueryParams', 'onQueryInput',
    'switchToTab', 'startTitleEdit', 'loadLexer', 'tokenize'];
const code = FUNCS.map(f => extractTopLevel(new RegExp('^(async )?function ' + f + '\\('), f)).join('\n');

/// The parameter-input DOM `updateQueryParams` rebuilds: real enough for element creation,
/// value carry-over, id registration and the (synthetic vs trusted) 'input' listener dispatch.
const param_dom = {};
function makeEl(tag)
{
    return {
        tagName: tag, style: {}, children: [], listeners: {},
        value: '', id: '', className: '', textContent: '', placeholder: '',
        appendChild(c) { this.children.push(c); },
        setAttribute() {},
        addEventListener(type, fn) { (this.listeners[type] = this.listeners[type] || []).push(fn); },
        dispatchEvent(ev) { for (const fn of (this.listeners[ev.type] || []).slice()) fn.call(this, ev); },
    };
}

const sandbox = {
    console, URL, TextEncoder, TextDecoder, JSON, Object, Array, Promise, Math, Set, Map,
    btoa: s => Buffer.from(s, 'binary').toString('base64'),
    atob: s => Buffer.from(s, 'base64').toString('binary'),
    /// Synthetic events the page dispatches (input resize / restore) are NOT trusted, like a
    /// browser's constructed Event; the trusted param-edit case dispatches a plain object with
    /// `isTrusted: true` instead.
    Event: class Event { constructor(type) { this.type = type; this.isTrusted = false; } },
    /// `scheduleSave` must arm its debounce timer without the save ever firing, so the run stays
    /// deterministic and node exits cleanly.
    setTimeout: () => 1,
    clearTimeout: () => {},
    tabs: [], activeTabId: null, tabSeq: 0, tabTitleSeq: 0,
    activeTitleCommit: null,
    /// The selection APIs the inline title editor uses to preselect the title being renamed.
    getSelection: () => ({ removeAllRanges() {}, addRange() {} }),
    activation_num: 0, params_restore_pending_token: null,
    run_epoch: 'ep', run_seq: 0,
    editorInteractionGen: 0,
    save_timer: null, column_color_modes: {}, pinned_columns: Object.create(null),
    url_pinned_columns: null, pinned_columns_url_malformed: false,
    url_color_modes: null, color_modes_url_malformed: false,
    run_immediately: true,
    defer_run_for_reconcile: true,
    deferred_run_cancelled: false,
    restoring_connection_from_history: false,
    opened_locally: false,
    user_elem: { value: '' },
    query_area: { value: '', selectionStart: 0, selectionEnd: 0, focus() {}, dispatchEvent() {}, setSelectionRange(s, e) { this.selectionStart = s; this.selectionEnd = e; } },
    document: {
        title: '', documentElement: { style: { setProperty() {} } },
        getElementById: id =>
        {
            if (id && id.startsWith('param-'))
                return Object.prototype.hasOwnProperty.call(param_dom, id) ? param_dom[id] : null;
            return { style: {}, innerHTML: '' };
        },
        createElement: tag => makeEl(tag),
        createRange: () => ({ selectNodeContents() {} }),
        querySelectorAll: () => [],
        body: { scrollTo() {} },
    },
    /// The container the rebuilt inputs land in: registers every descendant carrying an id so
    /// `document.getElementById('param-...')` finds the live inputs, exactly like the real DOM.
    queryParamsContainer: {
        set innerHTML(v) { for (const k of Object.keys(param_dom)) delete param_dom[k]; },
        get innerHTML() { return ''; },
        appendChild(wrapper) { (function reg(el) { if (el.id) param_dom[el.id] = el; for (const c of el.children || []) reg(c); })(wrapper); },
    },
    location: { origin: 'http://localhost:8123', pathname: '/play', href: 'http://localhost:8123/play' },
    /// Rendering the extracted functions touch: no DOM in the harness.
    renderTabBar: () => {},
    refreshFavicon: () => {},
    updateTabIndicator: () => {},
    queryToColor: () => '',
    applySelectedDatabaseHighlight: () => {},
    applyColumnColors: () => {},
    applyPinnedColumns: () => {},
    updateQueryBackdrop: () => {},
    resizeQueryAreaIfSlightlyOverflowing: () => {},
    getQueryBoundaries: () => [],
    focusEditorForRun: () => {},
    updateRunButtons: () => {},
    anyInFlight: () => false,
    abortTabQuery: tab => { if (tab) ++tab.reqNum; },
    /// The per-tab editor restore: activating a tab hands the shared editor + parameter inputs to
    /// the real `restoreEditor`, which owns the rebuild via `params_restore_pending_token`.
    activateTab: async id =>
    {
        const tab = sandbox.tabs.find(t => t.id === id);
        if (!tab) return;
        sandbox.activeTabId = id;
        const token = ++sandbox.activation_num;
        await sandbox.restoreEditor({ query: tab.query, params: tab.params }, token);
    },
    selected_database: null,
    selected_database_connection: null,
    server_current_database: null,
    currentQueryParams: [],
    updateQueryParamsGeneration: 0,
    lexer_module: undefined,
    bootstrap_dirty: false,
    bootstrap_settled: true,   /// post-startup: reconciliation is over, edits are ordinary
    has_url_query: false,
    url_tab_name: null,
    url_query: '',
    TAB_STORE: 'tabs',
    META_STORE: 'meta',
};
sandbox.url_elem = { value: sandbox.location.origin };
sandbox.password_elem = { value: '' };
sandbox.location.toString = function() { return this.href; };
sandbox.window = sandbox;
sandbox.current_url = new URL(sandbox.location.href);
sandbox.history = {
    stack: [], idx: -1,
    get state() { return this.idx >= 0 ? this.stack[this.idx].state : null; },
    _sync() { if (this.idx >= 0) sandbox.location.href = new URL(this.stack[this.idx].url, sandbox.location.origin).href; },
    pushState(state, title, url) { this.stack.length = this.idx + 1; this.stack.push({ state, url }); this.idx++; this._sync(); },
    replaceState(state, title, url)
    {
        if (this.idx < 0) { this.stack.push({ state, url }); this.idx = 0; }
        else this.stack[this.idx] = { state, url };
        this._sync();
    },
};
/// Minimal in-memory IndexedDB supporting exactly what `persist` calls (armed but never fired).
const idb_stores = {};
function fakeObjectStore(name)
{
    if (!idb_stores[name]) idb_stores[name] = new Map();
    const map = idb_stores[name];
    const keyOf = v => (v.id !== undefined ? v.id : v.key);
    return {
        clear() { map.clear(); },
        put(v) { map.set(keyOf(v), v); },
        getAll() { return { result: [...map.values()] }; },
        get(k) { return { result: map.get(k) }; },
    };
}
sandbox.dbReady = Promise.resolve({
    objectStoreNames: { contains: () => true },
    transaction()
    {
        const tx = { objectStore: fakeObjectStore };
        Promise.resolve().then(() => { if (tx.oncomplete) tx.oncomplete(); });
        return tx;
    },
});
vm.createContext(sandbox);
vm.runInContext(code, sandbox, { filename: 'play-extract.js' });

const drain = () => new Promise(resolve => setImmediate(resolve));
const active = () => sandbox.tabs.find(t => t.id === sandbox.activeTabId);
const curUrl = () => sandbox.history.stack[sandbox.history.idx].url;
const curState = () => sandbox.history.stack[sandbox.history.idx].state;

function assert_eq(label, actual, expected)
{
    if (actual !== expected)
    {
        console.error('FAIL: ' + label + ': expected ' + JSON.stringify(expected) + ', got ' + JSON.stringify(actual));
        process.exit(1);
    }
    console.log(label);
}

/// Compare parameter objects by a canonical (key-sorted) serialization.
const canon = obj => JSON.stringify(Object.fromEntries(Object.entries(obj || {}).sort()));
function assert_params(label, actual, expected)
{
    assert_eq(label, canon(actual), canon(expected));
}

/// A keystroke: drives the REAL `input` listener (`onQueryInput`) — including its internal
/// ordering of the in-memory tab sync vs the input rebuild, which the empty-query fast path
/// depends on. Await the returned rebuild promise to let the rebuild land, or keep it pending
/// to model an edit racing a run/completion.
function type(q)
{
    sandbox.query_area.value = q;
    return sandbox.onQueryInput({ type: 'input', isTrusted: true });
}

/// A trusted edit of a live `param_*` input: drives the REAL listener `updateQueryParams`
/// installed on the rebuilt input (tab-owned params + `syncHistory`).
function setTrustedParam(name, value)
{
    const input = param_dom['param-' + name];
    if (!input) throw new Error('no live input for param ' + name + '; the rebuild did not create it');
    input.value = value;
    input.dispatchEvent({ type: 'input', isTrusted: true });
}

/// The editable title `span` the tab bar hands to `startTitleEdit`: real enough for the editor's
/// class/attribute bookkeeping, its preselection and its `keydown`/`blur` commit listeners.
function makeTitleEl(text)
{
    const el = makeEl('span');
    el.textContent = text;
    el.contentEditable = 'false';
    el.classList = { add() {}, remove() {} };
    el.removeAttribute = () => {};
    el.closest = () => ({ classList: { add() {}, remove() {} } });
    el.focus = () => {};
    return el;
}

/// A tab rename: drives the REAL inline title editor (`startTitleEdit`) and commits it with
/// Enter, exactly as the tab bar does.
function rename(tab, title)
{
    const titleEl = makeTitleEl(tab.title);
    sandbox.startTitleEdit(tab, titleEl);
    titleEl.textContent = title;
    titleEl.dispatchEvent({ type: 'keydown', key: 'Enter', preventDefault() {}, stopPropagation() {} });
}

/// A run's launch, exactly as `postOne`/`postAll` bind it: snapshot the parameter VALUES
/// synchronously at launch (`resolveRunParams` — live inputs, or the tab's saved params during a
/// pending restore), then bind the parameter NAMES to the run's own tokenization of the launched
/// text (`extractRunParamNames` + `pickRunParams`), falling back to the merged sources.
async function startRun(q)
{
    const tab = active();
    const pending = sandbox.params_restore_pending_token !== null;
    const param_values_at_launch = sandbox.resolveRunParams(pending, tab);
    const param_sources = Object.assign({}, tab.params || {}, param_values_at_launch);
    tab.launchQuery = q;
    tab.launchRunId = sandbox.nextRunId();
    const run_params = await sandbox.extractRunParamNames(q);
    const snapshot = run_params === null ? param_sources : sandbox.pickRunParams(run_params, param_sources);
    return { query: q, params: snapshot };
}

/// Complete a run started with `startRun`: `saveHistory` receives the LAUNCH-TIME snapshot,
/// never whatever the editor/params hold by the time the response arrives. Synchronous, so a
/// test can order it deterministically against a still-pending input rebuild.
function finishRun(started)
{
    sandbox.saveHistory({ query: started.query, resultQuery: started.query, params: started.params, fullEditor: true, format: 'JSONCompact', ok: true, data: 'result of ' + started.query, elapsed_ns: 1,
        database: sandbox.selected_database, url: sandbox.url_elem.value, user: sandbox.user_elem.value });
}

/// A settled successful run of the editor text: edit lands, then launch + complete.
async function run(q)
{
    await type(q);
    finishRun(await startRun(q));
    await drain();
}

function reset()
{
    sandbox.tabs.length = 0;
    sandbox.tabSeq = 0;
    sandbox.tabTitleSeq = 0;
    sandbox.history.stack.length = 0;
    sandbox.history.idx = -1;
    sandbox.query_area.value = '';
    sandbox.currentQueryParams = [];
    for (const k of Object.keys(param_dom)) delete param_dom[k];
    sandbox.document.title = '';
    sandbox.deferred_run_cancelled = false;
    sandbox.activeTitleCommit = null;
    const tab = sandbox.makeTab();
    sandbox.tabs.push(tab);
    sandbox.activeTabId = tab.id;
}

(async () =>
{
    /// A query edit that REMOVES a placeholder: the rebuild folds the surviving inputs into
    /// `tab.params` (`syncParamsAfterRebuild`) and refreshes the entry, so the removed binding
    /// leaks into neither the URL nor the entry, and a clean rerun of the new text keeps `run=1`.
    reset();
    await type('SELECT {x:Int32}');
    setTrustedParam('x', '1');
    await run('SELECT {x:Int32}');
    assert_eq('param run: the clean parameterized run carries run=1', curUrl().includes('run=1'), true);
    assert_eq('param run: the URL carries the binding', curUrl().includes('param_x'), true);
    await type('SELECT 1');
    assert_params('removal edit: the removed placeholder leaves tab.params', active().params, {});
    assert_eq('removal edit: the refreshed entry drops the stale param', curUrl().includes('param_x'), false);
    assert_eq('removal edit: the diverged editor drops run=1', curUrl().includes('run=1'), false);
    await run('SELECT 1');
    assert_eq('clean rerun after removal: run=1 is kept', curUrl().includes('run=1'), true);
    assert_eq('clean rerun after removal: no stale param_x in the URL', curUrl().includes('param_x'), false);
    assert_params('clean rerun after removal: the entry params are empty', curState().params, {});

    /// Clearing a parameterized query to EMPTY text: `updateQueryParams`'s empty-query fast path
    /// refreshes the current history entry SYNCHRONOUSLY (no tokenization await), so the real
    /// input listener (`onQueryInput`) must sync `tab.query` BEFORE kicking off the rebuild —
    /// otherwise the entry/URL is rewritten from the stale PRE-edit query, and copying the URL or
    /// reloading right after the clear resurrects the query the user just cleared. Assert
    /// IMMEDIATELY, before the returned rebuild promise is awaited: the URL/hash must already be
    /// clear, not only after a later persist/reload.
    reset();
    const cleared_b64 = sandbox.toBase64('SELECT {x:Int32}');
    await type('SELECT {x:Int32}');
    setTrustedParam('x', '1');
    await run('SELECT {x:Int32}');
    assert_eq('clear to empty: the baseline URL carries the query', curUrl().includes(cleared_b64), true);
    const clear_rebuild = type('');   /// deliberately not awaited: the fast path is synchronous
    assert_eq('clear to empty: the entry query is cleared immediately', curState().query, '');
    assert_eq('clear to empty: the URL drops the cleared query immediately', curUrl().includes(cleared_b64), false);
    assert_eq('clear to empty: no stale param_x in the URL', curUrl().includes('param_x'), false);
    assert_eq('clear to empty: no run=1 on the cleared draft', curUrl().includes('run=1'), false);
    assert_params('clear to empty: tab.params is emptied immediately', active().params, {});
    await clear_rebuild;
    await drain();
    assert_eq('clear to empty: the draft stays empty after the rebuild settles', active().query, '');
    assert_eq('clear to empty: the URL stays clear after the rebuild settles', curUrl().includes(cleared_b64), false);

    /// REORDERING placeholders is not a change of bindings: a clean run of the reordered text
    /// keeps run=1 and both values, and the tab still reads as clean for the editor-only writers.
    reset();
    await type('SELECT {x:Int32}, {y:Int32}');
    setTrustedParam('x', '1');
    setTrustedParam('y', '2');
    await run('SELECT {x:Int32}, {y:Int32}');
    assert_eq('reorder baseline: the clean run carries run=1', curUrl().includes('run=1'), true);
    await type('SELECT {y:Int32}, {x:Int32}');
    await run('SELECT {y:Int32}, {x:Int32}');
    assert_eq('reordered clean run: run=1 is kept', curUrl().includes('run=1'), true);
    assert_eq('reordered clean run: x keeps its value', curUrl().includes('param_x=1'), true);
    assert_eq('reordered clean run: y keeps its value', curUrl().includes('param_y=2'), true);
    assert_eq('reordered clean run: the tab still reflects its run', sandbox.tabReflectsRun(active()), true);
    sandbox.syncHistory();   /// what a tab switch/rename invokes for the marker decision
    assert_eq('reordered clean run: an editor-only rewrite keeps run=1', curUrl().includes('run=1'), true);

    /// Reorder-and-run before the input rebuild lands: the launch snapshot follows the launched
    /// text's placeholder order while `tab.params` still keeps the previous order — same
    /// bindings, so run=1 must survive the order-insensitive comparison.
    reset();
    await type('SELECT {x:Int32}, {y:Int32}');
    setTrustedParam('x', '1');
    setTrustedParam('y', '2');
    await run('SELECT {x:Int32}, {y:Int32}');
    const reorder_rebuild = type('SELECT {y:Int32}, {x:Int32}');
    finishRun(await startRun('SELECT {y:Int32}, {x:Int32}'));
    await drain();
    assert_eq('immediate reordered run: run=1 is kept', curUrl().includes('run=1'), true);
    await reorder_rebuild;
    assert_eq('immediate reordered run: the tab reads clean once the rebuild lands', sandbox.tabReflectsRun(active()), true);

    /// A placeholder-removing edit while a run is IN FLIGHT, rebuild landing FIRST: the
    /// completion must not resurrect the removed binding under the newer draft, and the draft
    /// entry carries no run=1.
    reset();
    await type('SELECT {x:Int32}');
    setTrustedParam('x', '1');
    await run('SELECT {x:Int32}');
    const in_flight = await startRun('SELECT {x:Int32}');
    await type('SELECT 1');
    assert_params('mid-flight removal: tab.params follows the draft', active().params, {});
    finishRun(in_flight);
    await drain();
    assert_eq('mid-flight removal: the draft entry has no stale param_x', curUrl().includes('param_x'), false);
    assert_eq('mid-flight removal: the draft entry has no run=1', curUrl().includes('run=1'), false);
    assert_eq('mid-flight removal: the completed result is kept', active().result && active().result.query, 'SELECT {x:Int32}');

    /// Same edit, but the COMPLETION wins the race (the rebuild is still awaiting tokenization
    /// when the response lands): the entry the completion writes must ALREADY be clean — no
    /// window where the URL pairs the new draft with the removed placeholder's binding (a reload
    /// or copied link taken then would resurrect it). `saveHistory` prunes the stale map by a
    /// textual placeholder scan of the draft; the landing rebuild then re-folds the inputs
    /// authoritatively (`syncParamsAfterRebuild` -> `refreshCurrentHistoryEntry`).
    reset();
    await type('SELECT {x:Int32}');
    setTrustedParam('x', '1');
    await run('SELECT {x:Int32}');
    const in_flight_late = await startRun('SELECT {x:Int32}');
    const rebuild = type('SELECT 1');   /// kicked off, deliberately not awaited yet
    finishRun(in_flight_late);          /// synchronous: writes before the rebuild lands
    assert_eq('completion before rebuild: the raced entry has no run=1', curUrl().includes('run=1'), false);
    assert_eq('completion before rebuild: no stale param_x even before the rebuild lands', curUrl().includes('param_x'), false);
    assert_params('completion before rebuild: tab.params is pruned to the draft text', active().params, {});
    await rebuild;
    await drain();
    assert_params('completion before rebuild: the landing rebuild folds tab.params', active().params, {});
    assert_eq('completion before rebuild: the landing rebuild repairs the entry', curUrl().includes('param_x'), false);
    assert_eq('completion before rebuild: the repaired entry still has no run=1', curUrl().includes('run=1'), false);
    assert_eq('completion before rebuild: the draft is intact', active().query, 'SELECT 1');
    assert_eq('completion before rebuild: the completed result is kept', active().result && active().result.query, 'SELECT {x:Int32}');

    /// Control for the textual prune: a mid-flight edit that KEEPS the placeholder must keep its
    /// binding in the completion's entry — the conservative scan only drops names that are
    /// certainly gone from the draft text.
    reset();
    await type('SELECT {x:Int32}');
    setTrustedParam('x', '1');
    await run('SELECT {x:Int32}');
    const in_flight_keep = await startRun('SELECT {x:Int32}');
    const rebuild_keep = type('SELECT {x:Int32} + 1');   /// kicked off, deliberately not awaited yet
    finishRun(in_flight_keep);                           /// synchronous: writes before the rebuild lands
    assert_eq('completion before rebuild (kept placeholder): the binding stays in the URL', curUrl().includes('param_x=1'), true);
    assert_eq('completion before rebuild (kept placeholder): the diverged draft still has no run=1', curUrl().includes('run=1'), false);
    await rebuild_keep;
    await drain();
    assert_params('completion before rebuild (kept placeholder): the landing rebuild keeps the binding', active().params, { x: '1' });
    assert_eq('completion before rebuild (kept placeholder): the draft is intact', active().query, 'SELECT {x:Int32} + 1');

    /// A query edit racing a PENDING tab restore: switching to a saved tab kicks off
    /// `restoreEditor`, whose tokenization the quick edit supersedes — the restore falls back to
    /// the tab's saved `params` and clears the stale inputs, and it is the EDIT's rebuild that
    /// recreates them. That rebuild must seed a placeholder with no surviving input from the
    /// tab's own saved snapshot: seeding from the (now empty) live inputs alone would rebuild
    /// `param-y` blank, and `syncParamsAfterRebuild` would fold the blank into `tab.params` and
    /// persist the loss of a binding the user never touched. The interleaving is pinned by
    /// gating the EDIT's tokenization (captured per call) until the superseded restore has
    /// committed its fallback, so the rebuild deterministically lands after the restore's tail.
    reset();
    await type('SELECT {x:Int32}');
    setTrustedParam('x', '1');
    await run('SELECT {x:Int32}');
    const saved_tab = sandbox.makeTab();
    saved_tab.query = 'SELECT {y:Int32}';
    saved_tab.params = { y: '2' };
    sandbox.tabs.push(saved_tab);
    const real_tokenize = sandbox.tokenize;
    let tokenize_hold = null;
    sandbox.tokenize = async q =>
    {
        const hold = tokenize_hold;                    /// captured at CALL time, per tokenization
        const tokens = await real_tokenize(q);
        if (hold) await hold;
        return tokens;
    };
    const pending_restore = sandbox.activateTab(saved_tab.id);      /// restore's tokenize: not gated
    let release_edit_rebuild;
    tokenize_hold = new Promise(resolve => { release_edit_rebuild = resolve; });
    const racing_rebuild = type('SELECT {y:Int32} -- edited');      /// supersedes the restore, rebuild gated
    await pending_restore;                                          /// fallback committed: tab.params = saved, inputs cleared
    assert_params('edit during pending restore: the fallback keeps the saved snapshot', active().params, { y: '2' });
    release_edit_rebuild();                                         /// now the edit's rebuild lands
    await racing_rebuild;
    await drain();
    sandbox.tokenize = real_tokenize;
    assert_params('edit during pending restore: the saved binding survives the rebuild', active().params, { y: '2' });
    assert_eq('edit during pending restore: the rebuilt input carries the saved value', param_dom['param-y'] && param_dom['param-y'].value, '2');
    assert_eq('edit during pending restore: the draft is intact', active().query, 'SELECT {y:Int32} -- edited');
    assert_eq('edit during pending restore: the entry never persists a blanked binding', curUrl().includes('param_y=&') || curUrl().endsWith('param_y='), false);

    /// A placeholder-removing edit followed by a STRUCTURAL leave (tab switch/add/duplicate/close)
    /// before the edit's input rebuild lands: `captureActiveTab` must not snapshot the removed
    /// placeholder's still-live input into `tab.params` — the entry `refreshCurrentHistoryEntry`
    /// writes for the tab being left would pair the new draft with the stale binding
    /// (`/play?param_x=1#SELECT 1`), and the activation that follows supersedes the pending
    /// rebuild (`updateQueryParamsGeneration`), so nothing would ever repair that entry. The
    /// capture prunes the DOM snapshot against the captured text (`queryMentionsParam`); a
    /// placeholder that SURVIVES the edit must keep its binding (conservative scan).
    reset();
    await type('SELECT {x:Int32}, {y:Int32}');
    setTrustedParam('x', '1');
    setTrustedParam('y', '2');
    await run('SELECT {x:Int32}, {y:Int32}');
    const other_tab = sandbox.makeTab();
    other_tab.query = 'SELECT 42';
    sandbox.tabs.push(other_tab);
    const left_tab = active();
    sandbox.tokenize = async q =>
    {
        const hold = tokenize_hold;                    /// captured at CALL time, per tokenization
        const tokens = await real_tokenize(q);
        if (hold) await hold;
        return tokens;
    };
    let release_structural_rebuild;
    tokenize_hold = new Promise(resolve => { release_structural_rebuild = resolve; });
    const structural_rebuild = type('SELECT {y:Int32}');   /// removes x; rebuild gated, not landed
    tokenize_hold = null;
    await sandbox.switchToTab(other_tab.id);               /// structural leave: capture + entry write
    const left_entry = sandbox.history.stack.find(e => e.state && e.state.tabId === left_tab.id);
    assert_params('structural leave before rebuild: the capture prunes the removed binding', left_tab.params, { y: '2' });
    assert_eq('structural leave before rebuild: no stale param_x in the left tab\'s entry', left_entry.url.includes('param_x'), false);
    assert_eq('structural leave before rebuild: the surviving binding stays in the entry', left_entry.url.includes('param_y=2'), true);
    assert_eq('structural leave before rebuild: the entry carries the new draft', left_entry.state.query, 'SELECT {y:Int32}');
    assert_eq('structural leave before rebuild: the diverged draft has no run=1', left_entry.url.includes('run=1'), false);
    release_structural_rebuild();                          /// the superseded rebuild lands and bails
    await structural_rebuild;
    await drain();
    sandbox.tokenize = real_tokenize;
    const left_entry_after = sandbox.history.stack.find(e => e.state && e.state.tabId === left_tab.id);
    assert_params('structural leave before rebuild: the superseded rebuild leaves the pruned map', left_tab.params, { y: '2' });
    assert_eq('structural leave before rebuild: the entry stays clean after the rebuild bails', left_entry_after.url.includes('param_x'), false);
    assert_eq('structural leave before rebuild: the target tab is unaffected', active().query, 'SELECT 42');

    /// A placeholder-removing edit made while the tab's OWN restore is still pending (a
    /// Back/Forward re-activation: the tab's entry is already current, and `restoreEditor` is
    /// awaiting tokenization), followed by a structural leave before the restore commits: the
    /// live `param_*` inputs are unsafe to read in that window (they still hold the previous
    /// tab's values), but the tab-owned snapshot must still be pruned against the captured text —
    /// otherwise the entry written for the left tab pairs the new draft with the saved binding of
    /// the removed placeholder (`/play?param_y=2#SELECT ...`), and the activation that follows
    /// supersedes both the restore's tail and the edit's rebuild, so nothing repairs it. A
    /// placeholder that SURVIVES the edit must keep its saved binding (conservative scan).
    reset();
    await run('SELECT 1');
    const home_tab = active();
    const revisit_tab = sandbox.makeTab();
    revisit_tab.query = 'SELECT {y:Int32}, {z:Int32}';
    revisit_tab.params = { y: '2', z: '3' };
    sandbox.tabs.push(revisit_tab);
    await sandbox.switchToTab(revisit_tab.id);             /// first visit: entry created, restore committed
    await sandbox.switchToTab(home_tab.id);                /// leave normally: a forward entry for the home tab
    const revisit_idx = sandbox.history.stack.findIndex(e => e.state && e.state.tabId === revisit_tab.id);
    sandbox.history.idx = revisit_idx;                     /// emulate Back: the tab's entry is current again
    sandbox.history._sync();
    sandbox.tokenize = async q =>
    {
        const hold = tokenize_hold;                        /// captured at CALL time, per tokenization
        const tokens = await real_tokenize(q);
        if (hold) await hold;
        return tokens;
    };
    let release_revisit_restore;
    tokenize_hold = new Promise(resolve => { release_revisit_restore = resolve; });
    const pending_revisit = sandbox.activateTab(revisit_tab.id);   /// what the popstate handler kicks off; held
    let release_revisit_edit;
    tokenize_hold = new Promise(resolve => { release_revisit_edit = resolve; });
    const revisit_edit = type('SELECT {z:Int32}');         /// removes y mid-restore; rebuild gated
    tokenize_hold = null;                                  /// the leave's own activation must not be gated
    await sandbox.switchToTab(home_tab.id);                /// structural leave during the pending restore
    const revisit_entry = sandbox.history.stack[revisit_idx];
    assert_params('leave during pending restore: the capture prunes the removed saved binding', revisit_tab.params, { z: '3' });
    assert_eq('leave during pending restore: no stale param_y in the left tab\'s entry', revisit_entry.url.includes('param_y'), false);
    assert_eq('leave during pending restore: the surviving saved binding stays in the entry', revisit_entry.url.includes('param_z=3'), true);
    assert_eq('leave during pending restore: the entry carries the new draft', revisit_entry.state.query, 'SELECT {z:Int32}');
    release_revisit_restore();                             /// the superseded restore's tail bails
    release_revisit_edit();                                /// the superseded rebuild bails
    await pending_revisit;
    await revisit_edit;
    await drain();
    sandbox.tokenize = real_tokenize;
    assert_params('leave during pending restore: the superseded tails leave the pruned map', revisit_tab.params, { z: '3' });
    assert_eq('leave during pending restore: the entry stays clean after the tails bail', sandbox.history.stack[revisit_idx].url.includes('param_y'), false);
    assert_eq('leave during pending restore: the home tab is restored intact', active().query, 'SELECT 1');

    /// A saved tab whose parameter inputs could NOT be rebuilt (`updateQueryParams` returned
    /// false: no WASM lexer, or a text too large to tokenize): `restoreEditor` clears the stale
    /// inputs and keeps the authoritative bindings only in the tab-owned snapshot. A structural
    /// leave (switch/add/close) then captures the tab with NO live `param_*` inputs — the capture
    /// must merge the tab-owned snapshot behind the (empty) live values, or it rewrites
    /// `tab.params`, the history entry and the `IndexedDB` copy to `{}`, and the next run loses a
    /// binding the user never touched. Once a later rebuild recreates the input, a live value
    /// still OVERRIDES the merged snapshot, so the merge cannot resurrect an edited-away value.
    reset();
    await run('SELECT 1');
    const lexerless_home = active();
    const lexerless_tab = sandbox.makeTab();
    lexerless_tab.query = 'SELECT {y:Int32}';
    lexerless_tab.params = { y: '2' };
    sandbox.tabs.push(lexerless_tab);
    sandbox.tokenize = async () => { throw new Error('lexer unavailable'); };   /// every rebuild fails -> params_ok == false
    /// The failed rebuild logs the expected diagnostic through `console.error`; swallow exactly
    /// that message so the test's stderr stays clean, while a genuine failure still reaches it.
    const real_console_error = console.error;
    console.error = (...a) => { if (!String(a[0]).startsWith('Tokenization failed')) real_console_error(...a); };
    await sandbox.switchToTab(lexerless_tab.id);           /// the restore commits the fallback: inputs cleared
    assert_params('unrebuildable restore: the fallback keeps the saved snapshot', active().params, { y: '2' });
    assert_eq('unrebuildable restore: no live input was rebuilt', param_dom['param-y'], undefined);
    await sandbox.switchToTab(lexerless_home.id);          /// structural leave: capture with no live inputs
    assert_params('unrebuildable restore: a structural leave preserves the saved bindings', lexerless_tab.params, { y: '2' });
    const lexerless_entry = sandbox.history.stack.find(e => e.state && e.state.tabId === lexerless_tab.id);
    assert_eq('unrebuildable restore: the saved binding stays in the left tab\'s entry', lexerless_entry.url.includes('param_y=2'), true);
    sandbox.tokenize = real_tokenize;                      /// the lexer is back: the next restore rebuilds the input
    console.error = real_console_error;
    await sandbox.switchToTab(lexerless_tab.id);
    assert_eq('unrebuildable restore: the recovered rebuild seeds the input from the snapshot', param_dom['param-y'] && param_dom['param-y'].value, '2');
    setTrustedParam('y', '5');
    await sandbox.switchToTab(lexerless_home.id);
    assert_params('unrebuildable restore: a live input overrides the merged snapshot', lexerless_tab.params, { y: '5' });

    /// A RENAME is a history writer too: committing the inline title editor rewrites the current
    /// entry (and the URL) for the active tab. A placeholder-removing edit whose input rebuild has
    /// not landed yet leaves the removed binding in `tab.params` — the live `param_*` input is only
    /// dropped by the asynchronous rebuild — so a rename in that window must not stamp
    /// `?param_x=1#SELECT 1` into the entry, resurrecting a parameter for a placeholder that no
    /// longer exists on reload or when the URL is copied. The rename goes through the same capture
    /// funnel as the other structural boundaries, which prunes the snapshot against the text
    /// (`queryMentionsParam`); a placeholder that SURVIVES the edit keeps its binding.
    reset();
    await type('SELECT {x:Int32}');
    setTrustedParam('x', '1');
    await run('SELECT {x:Int32}');
    assert_eq('rename baseline: the run URL carries the binding', curUrl().includes('param_x=1'), true);
    sandbox.tokenize = async q =>
    {
        const hold = tokenize_hold;                    /// captured at CALL time, per tokenization
        const tokens = await real_tokenize(q);
        if (hold) await hold;
        return tokens;
    };
    let release_rename_rebuild;
    tokenize_hold = new Promise(resolve => { release_rename_rebuild = resolve; });
    const rename_rebuild = type('SELECT 1');           /// removes x; rebuild gated, not landed
    tokenize_hold = null;
    rename(active(), 'renamed');
    assert_eq('rename before rebuild: the title is committed', active().title, 'renamed');
    assert_params('rename before rebuild: the capture prunes the removed binding', active().params, {});
    assert_eq('rename before rebuild: no stale param_x in the entry', curUrl().includes('param_x'), false);
    assert_eq('rename before rebuild: the entry carries the new draft', curState().query, 'SELECT 1');
    assert_eq('rename before rebuild: the diverged draft has no run=1', curUrl().includes('run=1'), false);
    release_rename_rebuild();                          /// the rebuild lands
    await rename_rebuild;
    await drain();
    sandbox.tokenize = real_tokenize;
    assert_params('rename before rebuild: the landing rebuild leaves the pruned map', active().params, {});
    assert_eq('rename before rebuild: the entry stays clean after the rebuild lands', curUrl().includes('param_x'), false);

    /// Control: a rename after an edit that KEEPS the placeholder must not drop its binding — the
    /// capture's scan only prunes names that are certainly gone from the text.
    reset();
    await type('SELECT {x:Int32}');
    setTrustedParam('x', '1');
    await run('SELECT {x:Int32}');
    await type('SELECT {x:Int32} + 1');
    rename(active(), 'kept');
    assert_params('rename with a kept placeholder: the binding survives', active().params, { x: '1' });
    assert_eq('rename with a kept placeholder: the entry keeps the binding', curUrl().includes('param_x=1'), true);
    assert_eq('rename with a kept placeholder: the entry carries the new draft', curState().query, 'SELECT {x:Int32} + 1');

    /// A trusted `param_*` edit is a history writer too: the listener on a live input persists the
    /// edited values into the active tab and rewrites the current entry (`syncHistory`). After a
    /// query edit that REMOVED a placeholder, the live inputs (and `currentQueryParams`) still
    /// describe the pre-edit text until the asynchronous rebuild lands — editing another value in
    /// that window must not serialize the removed placeholder's binding into the entry/URL
    /// (`/play?param_x=1&param_y=3#SELECT {y:Int32}`; a reload or copied link would resurrect it).
    /// The listener goes through the same capture funnel as the other writers, which prunes the
    /// snapshot against `tab.query` (`queryMentionsParam`); the edited binding itself survives.
    reset();
    await type('SELECT {x:Int32} + {y:Int32}');
    setTrustedParam('x', '1');
    setTrustedParam('y', '2');
    await run('SELECT {x:Int32} + {y:Int32}');
    assert_eq('param edit baseline: the run URL carries both bindings', curUrl().includes('param_x=1') && curUrl().includes('param_y=2'), true);
    sandbox.tokenize = async q =>
    {
        const hold = tokenize_hold;                    /// captured at CALL time, per tokenization
        const tokens = await real_tokenize(q);
        if (hold) await hold;
        return tokens;
    };
    let release_param_edit_rebuild;
    tokenize_hold = new Promise(resolve => { release_param_edit_rebuild = resolve; });
    const param_edit_rebuild = type('SELECT {y:Int32}');   /// removes x; rebuild gated, not landed
    tokenize_hold = null;
    setTrustedParam('y', '3');                             /// the stale param-x input is still live
    assert_params('param edit before rebuild: the capture prunes the removed binding', active().params, { y: '3' });
    assert_eq('param edit before rebuild: no stale param_x in the entry', curUrl().includes('param_x'), false);
    assert_eq('param edit before rebuild: the edited binding reaches the URL', curUrl().includes('param_y=3'), true);
    assert_eq('param edit before rebuild: the entry carries the new draft', curState().query, 'SELECT {y:Int32}');
    assert_eq('param edit before rebuild: the diverged draft has no run=1', curUrl().includes('run=1'), false);
    release_param_edit_rebuild();                          /// the rebuild lands
    await param_edit_rebuild;
    await drain();
    sandbox.tokenize = real_tokenize;
    assert_params('param edit before rebuild: the landing rebuild keeps the edited binding', active().params, { y: '3' });
    assert_eq('param edit before rebuild: the entry stays clean after the rebuild lands', curUrl().includes('param_x'), false);

    console.log('OK');
})().catch(e => { console.error('FAIL: ' + (e && e.stack || e)); process.exit(1); });
EOF

node "$harness" "$html"
