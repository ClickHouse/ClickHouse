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
#     the stale binding under the newer draft — whichever side wins the race (the rebuild lands
#     before the completion, or the completion writes first and the landing rebuild repairs the
#     entry in place via `refreshCurrentHistoryEntry`).
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
    'writeHistoryEntry', 'sameParamValues', 'tabReflectsRun', 'liveDivergedFromRun',
    'effectiveDatabase', 'sameServerAddress', 'effectiveConnectionUser',
    'stampSelectedDatabaseConnection', 'refreshCurrentHistoryEntry', 'saveHistory', 'syncHistory',
    'markBootstrapDirty', 'scheduleSave', 'persist', 'restoreEditor',
    'extractQueryParams', 'getParamValues', 'setParamValues', 'pickRunParams', 'resolveRunParams',
    'extractRunParamNames', 'syncParamsAfterRebuild', 'updateQueryParams', 'loadLexer', 'tokenize'];
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

/// A keystroke: the real `input` listener syncs only the in-memory tab and kicks off the async
/// input rebuild (`updateQueryParams`). Await the returned promise to let the rebuild land, or
/// keep it pending to model an edit racing a run/completion.
function type(q)
{
    sandbox.query_area.value = q;
    const tab = active();
    if (tab) tab.query = q;
    return sandbox.updateQueryParams();
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
    /// when the response lands): the entry written from the then-stale `tab.params` is repaired
    /// in place once the rebuild lands (`syncParamsAfterRebuild` -> `refreshCurrentHistoryEntry`).
    reset();
    await type('SELECT {x:Int32}');
    setTrustedParam('x', '1');
    await run('SELECT {x:Int32}');
    const in_flight_late = await startRun('SELECT {x:Int32}');
    const rebuild = type('SELECT 1');   /// kicked off, deliberately not awaited yet
    finishRun(in_flight_late);          /// synchronous: writes before the rebuild lands
    assert_eq('completion before rebuild: the raced entry has no run=1', curUrl().includes('run=1'), false);
    await rebuild;
    await drain();
    assert_params('completion before rebuild: the landing rebuild folds tab.params', active().params, {});
    assert_eq('completion before rebuild: the landing rebuild repairs the entry', curUrl().includes('param_x'), false);
    assert_eq('completion before rebuild: the repaired entry still has no run=1', curUrl().includes('run=1'), false);
    assert_eq('completion before rebuild: the draft is intact', active().query, 'SELECT 1');
    assert_eq('completion before rebuild: the completed result is kept', active().result && active().result.query, 'SELECT {x:Int32}');

    console.log('OK');
})().catch(e => { console.error('FAIL: ' + (e && e.stack || e)); process.exit(1); });
EOF

node "$harness" "$html"
