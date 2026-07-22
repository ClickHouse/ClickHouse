#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs node (present in the stateless-test image, not in the fasttest one)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for the Web UI "history on run, not on keypress" contract in play.html.
# Keystrokes update only the in-memory tab (`tab.query`); browser history, the URL and the
# persisted workspace are recorded on a successful run (`saveHistory`) or a structural tab
# change (switch / add / close / rename). The three edits that implement the contract on top of
# the per-tab architecture are pinned here, each driving the real page functions under node:
#   - closing the ACTIVE tab folds its latest unrun draft into its own history entry before
#     removal (`closeTab` -> `captureActiveTab` + `refreshCurrentHistoryEntry`), so a later Back
#     that recreates the closed tab restores the draft, not the stale last-run snapshot, and the
#     refreshed entry drops `run=1` (the draft was never run);
#   - a same-session Back-then-Forward round-trip preserves a newer unrun draft instead of
#     clobbering it with the entry's older query (`window.onpopstate`'s `preserve_draft`). "Draft"
#     means dirty since the tab's last history write (`tab.query !== tab.lastSavedQuery`), NOT
#     `!tabReflectsRun`, so a clean `Run selected` (whose result snapshots only the selected
#     statement) is not mistaken for a draft and Back/Forward restore its entry queries verbatim;
#     a clean editor likewise keeps restoring entries — queries and params alike — verbatim;
#   - typing does not cancel an in-flight run, so a delayed completion must not clobber a newer,
#     unrun draft (or a live parameter edit) typed while the run was still in flight: `saveHistory`
#     leaves the live editor/params alone (`tab.query`/`tab.params` diverged from `tab.launchQuery`
#     / the launch-time params) and drops `run=1` on the entry it writes, while still keeping the
#     completed run's own result snapshot;
#   - the reload path (`persist` -> `reconcileStartup`): a debounced save that flushes an unrun
#     draft diverging from the URL (fired by a later structural event / Back-Forward / color
#     toggle) makes a reload restore that draft as editor text but NEVER auto-run it (the
#     stale-reload branch, `preserve_local_query`, refuses the URL's `run=1`); a clean run, by
#     contrast, is both restored and re-run on reload.
# The harness extracts the real tab/history functions from the served /play page and drives them
# under node with stub DOM/history objects (including a minimal in-memory IndexedDB), asserting on
# the observable state: history entries, the active tab, the editor, the persisted workspace, and
# whether a reload auto-runs the restored query. The per-tab run path (`postOne`/`postAll`/
# `postSingle`/`postMulti` and the WASM-lexer preflight) is master's and has its own coverage; here
# a run is driven through the real `saveHistory` with the same launch-time snapshot those
# entrypoints capture, which is what the history contract turns on.

html="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_play.html"
harness="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_play_history_harness.js"

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

/// The real code under test: the tab/history bookkeeping, taken verbatim from the page.
/// DOM rendering, the per-tab run path and persistence I/O are stubbed below.
const FUNCS = ['toBase64', 'fromBase64', 'nextDefaultTitle', 'uniqueTitle', 'tabRuntimeDefaults',
    'makeTab', 'nextRunId', 'getActiveTab', 'captureActiveTab', 'buildHistoryParams',
    'writeHistoryEntry', 'sameParamValues', 'queryMentionsParam', 'tabReflectsRun', 'liveDivergedFromRun', 'effectiveDatabase',
    'sameServerAddress', 'effectiveConnectionUser', 'stampSelectedDatabaseConnection',
    'refreshCurrentHistoryEntry', 'saveHistory', 'syncHistory', 'resolveTabForState',
    'markBootstrapDirty', 'switchToTab', 'addTab', 'closeTab', 'scheduleSave', 'loadFromDb',
    'persist', 'persistColorModes', 'reconcileStartup'];
let code = FUNCS.map(f => extractTopLevel(new RegExp('^(async )?function ' + f + '\\('), f)).join('\n');
code += '\n' + extractTopLevel(/^window\.onpopstate = /, 'window.onpopstate');

const sandbox = {
    console, URL, TextEncoder, TextDecoder, JSON, Object, Array, Promise, Math, Set, Map,
    btoa: s => Buffer.from(s, 'binary').toString('base64'),
    atob: s => Buffer.from(s, 'base64').toString('binary'),
    /// `scheduleSave` must arm its debounce timer without the save ever firing (the reload cases
    /// force it explicitly via `persist`), so the run stays deterministic and node exits cleanly.
    setTimeout: () => 1,
    clearTimeout: () => {},
    /// Globals declared elsewhere in the page that the extracted functions read.
    tabs: [], activeTabId: null, tabSeq: 0, tabTitleSeq: 0,
    /// The per-tab run architecture's generation counters (master): a tab owns `reqNum`, and
    /// `activation_num` orders concurrent tab activations. `params_restore_pending_token` stays
    /// null (no activation mid-restore in these cases), so `captureActiveTab` captures params.
    activation_num: 0, params_restore_pending_token: null,
    /// Fresh run identities are minted per launch; a fixed epoch keeps them deterministic here.
    run_epoch: 'ep', run_seq: 0,
    editorInteractionGen: 0,
    save_timer: null, column_color_modes: {}, pinned_columns: Object.create(null),
    /// The `?pinned_columns=` / `?color_modes=` URL carriers `reconcileStartup` reads. No case here
    /// opens such a link, so they stay absent (null / not-malformed), like a plain `/play` load.
    url_pinned_columns: null, pinned_columns_url_malformed: false,
    url_color_modes: null, color_modes_url_malformed: false,
    /// `?run=1` propagation: the run=1 marker is stamped only for an entry produced by a genuine
    /// run whose editor still reflects it, through the tab's own `runnableUrl` policy bit —
    /// stamped by `saveHistory` from the load directive (`run_immediately`). Set true so a clean
    /// run stamps run=1 and the reload cases can observe an unrun draft dropping it.
    run_immediately: true,
    defer_run_for_reconcile: true,
    deferred_run_cancelled: false,
    /// A Back/Forward that lands on a different connection replays connection-input events guarded
    /// by this flag; these cases never change the connection, so it stays false.
    restoring_connection_from_history: false,
    opened_locally: false,
    user_elem: { value: '' },
    query_area: { value: '', selectionStart: 0, selectionEnd: 0, focus() {}, dispatchEvent() {}, setSelectionRange(s, e) { this.selectionStart = s; this.selectionEnd = e; } },
    document: {
        title: '', documentElement: { style: { setProperty() {} } },
        getElementById: id =>
        {
            if (id && id.startsWith('param-'))
            {
                const name = id.slice('param-'.length);
                return Object.prototype.hasOwnProperty.call(sandbox.param_inputs, name)
                    ? { value: sandbox.param_inputs[name] }
                    : null;
            }
            return { style: {}, innerHTML: '' };
        },
        createElement: () => ({ style: {}, innerHTML: '', appendChild() {} }),
        querySelectorAll: () => [],
        body: { scrollTo() {} },
    },
    location: { origin: 'http://localhost:8123', pathname: '/play', href: 'http://localhost:8123/play' },
    /// The live parameter inputs, keyed by name; `getParamValues` snapshots them like the real
    /// one reads the `param_*` DOM inputs, `setParamValues` writes them back.
    param_inputs: {},
    getParamValues: () => ({ ...sandbox.param_inputs }),
    setParamValues: values => { if (values) for (const [k, v] of Object.entries(values)) sandbox.param_inputs[k] = v; },
    /// Result-panel / chrome rendering the history writers touch: no DOM in the harness.
    renderTabBar: () => {},
    refreshFavicon: () => {},
    updateTabIndicator: () => {},
    queryToColor: () => '',
    applySelectedDatabaseHighlight: () => {},
    applyColumnColors: () => {},
    applyPinnedColumns: () => {},
    updateQueryParams: async () => true,
    focusEditorForRun: () => {},
    updateRunButtons: () => {},
    anyInFlight: () => false,
    /// A run in a tab is stopped by bumping its generation and aborting its request; no live
    /// request here, so the token bump is all that matters.
    abortTabQuery: tab => { if (tab) ++tab.reqNum; },
    /// The per-tab editor restore. The real `activateTab` reveals the panel, re-materializes the
    /// stored result and repaints chrome; its history-contract-relevant effect is restoring the
    /// shared editor + parameter inputs from the tab (`restoreEditor`), which is modelled here.
    activateTab: async id =>
    {
        const tab = sandbox.tabs.find(t => t.id === id);
        if (!tab) return;
        sandbox.activeTabId = id;
        ++sandbox.activation_num;
        sandbox.query_area.value = tab.query;
        sandbox.param_inputs = { ...(tab.params || {}) };
    },
    /// The startup auto-run of a still-authoritative `run=1` URL. The reload cases assert this
    /// fires for a clean run but never for a restored unrun draft.
    postAll: async () => { sandbox.postAllCalled = true; },
    /// Connection/database state the merged history writers + Back/Forward restore read. These
    /// cases never select a database or change the connection, so the selection stays null (server
    /// default) and the live connection matches the producing one on every run (`liveDivergedFromRun`
    /// false), so `run=1` is preserved exactly. The server's current database is unknown to the
    /// harness, which `effectiveDatabase` treats as "no canonical default", leaving null === null.
    selected_database: null,
    selected_database_connection: null,
    server_current_database: null,
    currentQueryParams: [],
    bootstrap_dirty: false,
    bootstrap_settled: false,
    has_url_query: false,
    url_tab_name: null,
    url_query: '',
    postAllCalled: false,
    TAB_STORE: 'tabs',
    META_STORE: 'meta',
};
sandbox.url_elem = { value: sandbox.location.origin };
sandbox.password_elem = { value: '' };
sandbox.location.toString = function() { return this.href; };
sandbox.window = sandbox;
sandbox.current_url = new URL(sandbox.location.href);
/// Browser-history stub: a stack of entries; Back/Forward fire `onpopstate` with the state of
/// the entry navigated to, and syncing `location.href` to the current entry's URL like a browser.
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
    back() { if (this.idx <= 0) throw new Error('nothing to go back to'); this.idx--; this._sync(); sandbox.window.onpopstate({ state: this.stack[this.idx].state }); },
    forward() { if (this.idx >= this.stack.length - 1) throw new Error('nothing to go forward to'); this.idx++; this._sync(); sandbox.window.onpopstate({ state: this.stack[this.idx].state }); },
};
/// Minimal in-memory IndexedDB supporting exactly what `persist`/`loadFromDb` call.
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

/// A keystroke reaches the page as a DOM edit plus the 'input' listener syncing the active tab in
/// memory; browser history, the URL and the workspace are deliberately NOT touched (the contract).
function type(q)
{
    sandbox.query_area.value = q;
    const tab = active();
    if (tab) tab.query = q;
}

/// A trusted parameter edit: the real `param-` input handler persists the values into the active
/// tab and stamps the current query + params into a history entry via `syncHistory`.
function setParam(name, value)
{
    sandbox.param_inputs[name] = value;
    const tab = active();
    if (tab) { tab.params = sandbox.getParamValues(); sandbox.syncHistory(); }
}

/// Bind a run's launch-time parameters the way the real run path does: `postOne`/`postAll`
/// derive the parameter NAMES from the run's own tokenization of the launched text
/// (`extractRunParamNames`, so the map follows the TEXT's placeholder set and order) and bind
/// them to the values known for the launching tab (`pickRunParams` over the live inputs / the
/// tab's saved params). A placeholder whose input was never touched still contributes '' (the
/// rebuilt input exists and is empty), so a name is never silently dropped from the launch
/// snapshot just because no value was typed. The full pipeline (real tokenizer,
/// `updateQueryParams` rebuild, `resolveRunParams`) is pinned by the companion parameter test;
/// here the same binding is modelled over the stub inputs so the snapshots `saveHistory`
/// receives have the real shape.
function launchParams(q)
{
    const values = {};
    for (const m of q.matchAll(/\{([a-zA-Z_][a-zA-Z0-9_]*):/g))
    {
        const name = m[1];
        if (values[name] === undefined)
            values[name] = sandbox.param_inputs[name] !== undefined ? sandbox.param_inputs[name] : '';
    }
    return values;
}

/// A successful run: the editor holds the query, and `saveHistory` records the result snapshot and
/// the history entry, as the run path does. `postOne`/`postAll` snapshot the launched editor into
/// `tab.launchQuery` (and a fresh `launchRunId`) before the request, which `saveHistory` compares
/// the live draft against; mirror that here. A full-editor run may carry `run=1`.
async function run(q)
{
    type(q);
    const tab = active();
    tab.launchQuery = q;
    tab.launchRunId = sandbox.nextRunId();
    await sandbox.saveHistory({ query: q, resultQuery: q, params: launchParams(q), fullEditor: true, format: 'JSONCompact', ok: true, data: 'result of ' + q, elapsed_ns: 1,
        /// Stamp the live connection the run executed against, exactly as `postSingle`/`postMulti` do,
        /// so the snapshot is recognized as reproducible on the current connection (`liveDivergedFromRun`
        /// false) and the entry keeps `run=1`; an unstamped snapshot is treated as diverged (fail closed).
        database: sandbox.selected_database, url: sandbox.url_elem.value, user: sandbox.user_elem.value });
    await drain();
}

/// A "Run selected" execution: the editor holds the full text, but only the selected statement
/// produced the result, so `saveHistory` records the full editor in `tab.query` and the selected
/// statement in `tab.result.query` (`fullEditor: false`) — never auto-runnable.
async function runSelected(editorText, selectedStatement)
{
    type(editorText);
    const tab = active();
    tab.launchQuery = editorText;
    tab.launchRunId = sandbox.nextRunId();
    await sandbox.saveHistory({ query: editorText, resultQuery: selectedStatement, params: launchParams(editorText), fullEditor: false, format: 'JSONCompact', ok: true, data: 'result of ' + selectedStatement, elapsed_ns: 1,
        database: sandbox.selected_database, url: sandbox.url_elem.value, user: sandbox.user_elem.value });
    await drain();
}

/// Begin a run without completing it, capturing the query/params at launch time — exactly what
/// `postOne`/`postAll` snapshot into `tab.launchQuery` and `paramValuesSnapshot` before the network
/// round-trip. Pair with `finishRun` once the live tab/params have moved on (typing does not cancel
/// an in-flight request).
function startRun(q)
{
    type(q);
    const tab = active();
    tab.launchQuery = q;
    tab.launchRunId = sandbox.nextRunId();
    return { query: q, params: launchParams(q) };
}

/// Complete a run started with `startRun`: `saveHistory` receives the LAUNCH-TIME snapshot, never
/// whatever the editor/params hold by the time the response actually arrives.
async function finishRun(started)
{
    await sandbox.saveHistory({ query: started.query, resultQuery: started.query, params: started.params, fullEditor: true, format: 'JSONCompact', ok: true, data: 'result of ' + started.query, elapsed_ns: 1,
        database: sandbox.selected_database, url: sandbox.url_elem.value, user: sandbox.user_elem.value });
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
    sandbox.query_area.selectionStart = 0;
    sandbox.query_area.selectionEnd = 0;
    sandbox.param_inputs = {};
    sandbox.currentQueryParams = [];
    sandbox.document.title = '';
    /// Simulate a session opened from a `run=1` URL so a genuine run stamps `run=1` into its entry
    /// and the reload cases can observe that a restored unrun draft has it dropped. A real reload
    /// starts a fresh JS context; here `reload` re-derives these from the reloaded URL.
    sandbox.run_immediately = true;
    sandbox.defer_run_for_reconcile = true;
    /// Clear the session-accumulated deferred-run cancellation between cases.
    sandbox.deferred_run_cancelled = false;
    const tab = sandbox.makeTab();
    sandbox.tabs.push(tab);
    sandbox.activeTabId = tab.id;
}

/// Simulate a full page reload of the current history entry. The debounced workspace save is
/// forced to fire (the sandbox `setTimeout` never does) by calling `persist` explicitly — as a
/// real >400 ms wait would — then `reconcileStartup` reads the saved workspace back from the fake
/// IndexedDB and reconciles it against the URL, exactly the path a real reload takes. A fresh
/// document has the editor seeded from the URL hash and empty parameter inputs. `postAllCalled`
/// records whether the restored query was auto-run.
async function reload()
{
    await sandbox.persist();
    const cur = sandbox.history.stack[sandbox.history.idx];
    sandbox.current_url = new URL(cur.url, sandbox.location.origin);
    sandbox.url_query = (cur.state && cur.state.query) || '';
    sandbox.url_tab_name = sandbox.current_url.searchParams.get('tab');
    sandbox.has_url_query = sandbox.url_query.length > 0;
    sandbox.run_immediately = sandbox.current_url.searchParams.has('run');
    sandbox.defer_run_for_reconcile = sandbox.run_immediately;
    /// A real reload is a fresh JS context, so the session-accumulated "the deferred startup
    /// auto-run was cancelled" flag starts false (module init); reconcileStartup gates on it.
    sandbox.deferred_run_cancelled = false;
    sandbox.query_area.value = sandbox.url_query;
    sandbox.param_inputs = {};
    sandbox.bootstrap_dirty = false;
    sandbox.bootstrap_settled = false;
    sandbox.postAllCalled = false;
    sandbox.activation_num = 0;
    sandbox.params_restore_pending_token = null;
    sandbox.save_timer = null;
    await sandbox.reconcileStartup();
    await drain();
}

(async () =>
{
    /// A keystroke does NOT touch history / the URL: after a run, typing a draft leaves the
    /// current entry holding the RUN query (only `tab.query` moves in memory). The fold happens
    /// on the next run / structural change, not per keystroke.
    reset();
    await run('SELECT 1');
    const entriesAfterRun = sandbox.history.stack.length;
    const urlAfterRun = sandbox.history.stack[sandbox.history.idx].url;
    type('SELECT 1 -- draft');
    assert_eq('keystroke: no new history entry is pushed', sandbox.history.stack.length, entriesAfterRun);
    assert_eq('keystroke: the current entry still holds the run query', sandbox.history.stack[sandbox.history.idx].state.query, 'SELECT 1');
    assert_eq('keystroke: the URL is not rewritten', sandbox.history.stack[sandbox.history.idx].url, urlAfterRun);
    assert_eq('keystroke: only the in-memory tab moves', active().query, 'SELECT 1 -- draft');

    /// Closing the active tab folds its latest draft into its history entry, dropping run=1,
    /// and Back recreates the closed tab from that entry with the draft intact.
    reset();
    await run('SELECT 1');
    type('SELECT 1 -- draft');
    sandbox.closeTab(sandbox.activeTabId);
    await drain();
    assert_eq('close: the closed tab entry carries the latest draft', sandbox.history.stack[0].state.query, 'SELECT 1 -- draft');
    assert_eq('close: the draft entry does not carry run=1', sandbox.history.stack[0].url.includes('run=1'), false);
    sandbox.history.back();
    await drain();
    assert_eq('close+back: the recreated tab restores the draft', active().query, 'SELECT 1 -- draft');
    assert_eq('close+back: the run result snapshot is kept', active().result && active().result.query, 'SELECT 1');

    /// A same-session Back-then-Forward round-trip preserves a newer unrun draft instead of
    /// restoring the entries' older queries over it.
    reset();
    await run('SELECT 0');
    await run('SELECT 1');
    type('SELECT 2');
    sandbox.history.back();
    await drain();
    assert_eq('back: a newer unrun draft is preserved', active().query, 'SELECT 2');
    assert_eq('back: the editor shows the draft', sandbox.query_area.value, 'SELECT 2');
    assert_eq('back: the entry result snapshot is adopted', active().result && active().result.query, 'SELECT 0');
    sandbox.history.forward();
    await drain();
    assert_eq('forward: the draft survives the round-trip', active().query, 'SELECT 2');
    assert_eq('forward: the entry result snapshot is adopted', active().result && active().result.query, 'SELECT 1');

    /// The preserved draft carries its parameter bindings too: Back must not restore the older
    /// entry's params under the newer draft query. A parameter edit calls `syncHistory`, which
    /// stamps the current query + params into a history entry (so a param-edited draft is itself
    /// saved); a keystroke AFTER it makes the editor dirty again (`tab.query !== tab.lastSavedQuery`),
    /// so this draft — with its live bindings — is genuinely preserved across the round-trip.
    reset();
    await run('SELECT 0');
    type('SELECT {x:Int32}');
    setParam('x', '1');
    await run('SELECT {x:Int32}');
    type('SELECT {x:Int32} + {y:Int32}');
    setParam('y', '2');
    type('SELECT {x:Int32} + {y:Int32} -- edited');
    sandbox.history.back();
    await drain();
    assert_eq('param back: the draft query is preserved', active().query, 'SELECT {x:Int32} + {y:Int32} -- edited');
    assert_params('param back: the draft params are preserved', active().params, { x: '1', y: '2' });
    assert_params('param back: the param inputs keep the draft bindings', sandbox.param_inputs, { x: '1', y: '2' });
    sandbox.history.forward();
    await drain();
    assert_eq('param forward: the draft query survives the round-trip', active().query, 'SELECT {x:Int32} + {y:Int32} -- edited');
    assert_params('param forward: the draft params survive the round-trip', active().params, { x: '1', y: '2' });

    /// Control: with a clean editor (the tab still reflects its run), entry params keep restoring
    /// verbatim, including dropping bindings the older entry does not carry.
    reset();
    await run('SELECT 0');
    type('SELECT {x:Int32}');
    setParam('x', '1');
    await run('SELECT {x:Int32}');
    sandbox.history.back();
    await drain();
    assert_eq('clean param back: the entry query is restored', active().query, 'SELECT 0');
    assert_params('clean param back: the entry params are restored verbatim', active().params, {});
    sandbox.history.forward();
    await drain();
    assert_eq('clean param forward: the entry query is restored', active().query, 'SELECT {x:Int32}');
    assert_params('clean param forward: the entry params are restored verbatim', active().params, { x: '1' });

    /// Control: with a clean editor (no draft), Back/Forward restore entries verbatim.
    reset();
    await run('SELECT 0');
    await run('SELECT 1');
    sandbox.history.back();
    await drain();
    assert_eq('back with a clean editor: the entry query is restored', active().query, 'SELECT 0');
    sandbox.history.forward();
    await drain();
    assert_eq('forward with a clean editor: the entry query is restored', active().query, 'SELECT 1');

    /// A clean `Run selected` is NOT a draft. Its result snapshots only the selected statement, so
    /// `tabReflectsRun` is false while the editor keeps the full text, but the editor was not edited
    /// after the run. Back/Forward must restore the older / full-editor entry query, not mistake the
    /// run-backed editor for a newer unrun draft. The preserve check keys off `tab.query !==
    /// tab.lastSavedQuery` ("dirty since this entry was written"), which stays false here.
    reset();
    await run('SELECT 0');
    await runSelected('SELECT 1; SELECT 2', 'SELECT 2');
    sandbox.history.back();
    await drain();
    assert_eq('run-selected back: the older entry query is restored', active().query, 'SELECT 0');
    sandbox.history.forward();
    await drain();
    assert_eq('run-selected forward: the full run-selected editor is restored', active().query, 'SELECT 1; SELECT 2');

    /// But a genuine draft typed AFTER a `Run selected` is still preserved on Back: the edit makes
    /// `tab.query` diverge from `lastSavedQuery`, so the dirty check fires and the draft rides along.
    reset();
    await run('SELECT 0');
    await runSelected('SELECT 1; SELECT 2', 'SELECT 2');
    type('SELECT 1; SELECT 2; SELECT 3');
    sandbox.history.back();
    await drain();
    assert_eq('run-selected+draft back: the newer draft is preserved', active().query, 'SELECT 1; SELECT 2; SELECT 3');

    /// A run's completion must not clobber a newer, unrun draft typed while it was in flight: typing
    /// does not cancel a request, so by the time it resolves the editor may hold text unrelated to
    /// what ran. `saveHistory` compares the live `tab.query` against the launch-time `tab.launchQuery`
    /// and leaves the draft alone, keeping the completed run's own result snapshot but dropping
    /// `run=1` (that draft was never run).
    reset();
    await run('SELECT 0');
    const inFlight = startRun('SELECT 1');
    type('SELECT 2 -- typed while SELECT 1 was still in flight');
    await finishRun(inFlight);
    assert_eq('delayed completion: the live draft survives', active().query, 'SELECT 2 -- typed while SELECT 1 was still in flight');
    assert_eq("delayed completion: the completed run's result is still kept", active().result && active().result.query, 'SELECT 1');
    assert_eq('delayed completion: the entry does not carry run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);

    /// Same hazard when only the PARAMETERS changed while the query itself did not (a param widget
    /// edited during an in-flight run): the live parameter edit must survive, and must not be
    /// silently paired with the completed run's own (now stale) parameter values under `run=1`.
    reset();
    await run('SELECT 0');
    const inFlightParams = startRun('SELECT {x:Int32}');
    setParam('x', '9');
    await finishRun(inFlightParams);
    assert_eq('delayed completion (params changed): the query is kept', active().query, 'SELECT {x:Int32}');
    assert_params('delayed completion (params changed): the live param edit survives', active().params, { x: '9' });
    assert_eq('delayed completion (params changed): the entry does not carry run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);

    /// Reload after a preserved-draft Back/Forward round-trip. The debounced save persists the draft
    /// (query != lastSavedQuery, the shape `reconcileStartup` treats as a stale reload), so the
    /// reload restores the draft as editor text — but the stale-reload branch drops the URL's run=1,
    /// so the unrun draft is NOT auto-executed.
    reset();
    await run('SELECT 0');
    await run('SELECT 1');
    type('SELECT 2');
    sandbox.history.back();
    await drain();
    sandbox.history.forward();
    await drain();
    await reload();
    assert_eq('reload after a preserved draft: the draft is restored as editor text', active().query, 'SELECT 2');
    assert_eq('reload after a preserved draft: the draft is not auto-run', sandbox.postAllCalled, false);

    /// A stale reload — the URL hash is a tab's last-synced query, but IndexedDB holds a newer,
    /// unrun edit — restores the draft as editor text and refuses to auto-run the URL's stale
    /// `run=1` (an unrun, possibly destructive, edit must stay unrun); the draft's own re-synced
    /// entry is unstamped too, through the ordinary dirty check (`reconcileStartup`'s
    /// `preserve_local_query`; `tabReflectsRun` is false for the draft).
    reset();
    await run('SELECT 1');
    type('SELECT 2');
    await reload();
    assert_eq('stale reload: the draft is restored unrun', active().query, 'SELECT 2');
    assert_eq('stale reload: the draft is not auto-run', sandbox.postAllCalled, false);
    assert_eq('stale reload: the preserved draft entry is not stamped run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);

    /// Reload after a color-mode toggle over an unrun draft. `persistColorModes` is a result-only
    /// change, but its debounced save snapshots the live editor (`captureActiveTab`), so the draft is
    /// persisted and restored on reload — again only as editor text, never auto-run.
    reset();
    await run('SELECT 1');
    type('SELECT 2');
    sandbox.column_color_modes = { c: 'heatmap' };
    sandbox.persistColorModes();
    await reload();
    sandbox.column_color_modes = {};
    assert_eq('reload after a color toggle over a draft: the draft is restored as editor text', active().query, 'SELECT 2');
    assert_eq('reload after a color toggle over a draft: the draft is not auto-run', sandbox.postAllCalled, false);

    /// Control: reloading a clean run (no draft) restores the run's query AND re-runs it — the URL is
    /// still authoritative (run=1 preserved) — the "reload re-runs what you ran" behavior the draft
    /// cases above deliberately do not trigger.
    reset();
    await run('SELECT 0');
    await run('SELECT 1');
    await reload();
    assert_eq('reload of a clean run: the run query is restored', active().query, 'SELECT 1');
    assert_eq('reload of a clean run: the run query is auto-run', sandbox.postAllCalled, true);

    /// A stale reload preserves the draft (its entry unstamped) — but the suppression is scoped
    /// to those draft entries, not to the session: an EXPLICIT run of the draft afterwards is a
    /// genuine run under a `?run=1` load, so its entry carries `run=1` again and the next reload
    /// re-runs what the user actually executed.
    reset();
    await run('SELECT 1');
    type('SELECT 2');
    await reload();
    assert_eq('stale reload then rerun: the draft is restored unrun', active().query, 'SELECT 2');
    assert_eq('stale reload then rerun: the preserved draft entry has no run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);
    await run('SELECT 2');
    assert_eq('stale reload then rerun: the explicit rerun entry carries run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), true);
    await reload();
    assert_eq('stale reload then rerun: the rerun query is restored on the next reload', active().query, 'SELECT 2');
    assert_eq('stale reload then rerun: the rerun query is auto-run on the next reload', sandbox.postAllCalled, true);

    /// The suppression must not leak onto OTHER tabs either: after a stale reload preserved one
    /// tab's draft, switching into a clean run-backed tab whose own runs (under a `?run=1` load)
    /// made its entries auto-runnable keeps `run=1` on its URL. Conversely a clean tab that only
    /// ever ran under a PLAIN load stays unstamped — the `run=1` directive of one tab never makes
    /// another tab's URLs auto-runnable. Both policy bits ride the persisted workspace
    /// (`runnableUrl`), so they hold across the reload.
    reset();
    sandbox.run_immediately = false;   /// tab A's run happens under a plain (no run=1) load
    await run('SELECT 10');
    const plainTab = sandbox.activeTabId;
    sandbox.run_immediately = true;    /// the rest of the session behaves as a `?run=1` load
    sandbox.addTab();
    await drain();
    await run('SELECT 20');
    const runBackedTab = sandbox.activeTabId;
    sandbox.addTab();
    await drain();
    await run('SELECT 30');
    type('SELECT 31');
    await reload();
    assert_eq('stale reload then switch: the draft is restored unrun', active().query, 'SELECT 31');
    assert_eq('stale reload then switch: the preserved draft entry has no run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);
    await sandbox.switchToTab(runBackedTab);
    await drain();
    assert_eq('stale reload then switch: the clean run=1-backed tab keeps run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), true);
    await sandbox.switchToTab(plainTab);
    await drain();
    assert_eq('stale reload then switch: a tab never run under run=1 stays unstamped', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);

    console.log('OK');
})().catch(e => { console.error('FAIL: ' + (e && e.stack || e)); process.exit(1); });
EOF

node "$harness" "$html"
