#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs node (present in the stateless-test image, not in the fasttest one)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for the Web UI history contract: keystrokes update only the in-memory
# tab, and browser history entries are reconciled on runs and structural tab changes.
# Two paths regressed during review of this contract and are pinned here:
#   - closing the active tab must fold its latest unrun draft into its history entry,
#     so a later Back that recreates the closed tab restores the draft, not the stale
#     last-run snapshot (and the refreshed entry must drop the `run=1` marker);
#   - a same-session Back-then-Forward round-trip must preserve a newer unrun draft
#     instead of clobbering it with the entry's older query, while a clean editor
#     (no draft) keeps restoring entries verbatim;
#   - the preserved draft carries its parameter bindings too: Back must not restore the
#     older entry's params under the newer draft query (with a clean editor, entry params
#     keep restoring verbatim);
#   - the reload path (persist -> reconcileStartup): once any debounced save flushes an
#     unrun draft that diverges from the URL (the onpopstate save after a preserved-draft
#     Back/Forward, or persistColorModes over a draft), a reload restores that draft as
#     editor text but NEVER auto-runs it (the stale-reload branch drops the URL's run=1);
#     a clean run, by contrast, is both restored and re-run on reload.
# The harness extracts the real tab/history functions from the served /play page and
# drives them under node with stub DOM/history objects (including a minimal in-memory
# IndexedDB), asserting on the observable state: history entries, the active tab, the
# editor, the persisted workspace, and whether a reload auto-runs the restored query.

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
/// DOM rendering, persistence and query execution are stubbed below.
const FUNCS = ['toBase64', 'nextDefaultTitle', 'uniqueTitle', 'makeTab', 'getActiveTab',
    'captureActiveTab', 'invalidateInFlight', 'activateTab', 'closeTab', 'scheduleSave',
    'buildHistoryParams', 'writeHistoryEntry', 'tabReflectsRun', 'refreshCurrentHistoryEntry',
    'saveHistory', 'syncHistory', 'resolveTabForState',
    /// The persistence + startup-reconciliation surface exercised by the reload cases below.
    'loadFromDb', 'persist', 'persistColorModes', 'reconcileStartup'];
let code = FUNCS.map(f => extractTopLevel(new RegExp('^(async )?function ' + f + '\\('), f)).join('\n');
code += '\n' + extractTopLevel(/^window\.onpopstate = /, 'window.onpopstate');

const sandbox = {
    console, URL, TextEncoder, TextDecoder,
    btoa: s => Buffer.from(s, 'binary').toString('base64'),
    atob: s => Buffer.from(s, 'base64').toString('binary'),
    /// `scheduleSave` must arm its debounce timer without the save ever firing,
    /// so the run stays deterministic and node exits without pending timers.
    setTimeout: () => 1,
    /// Globals the extracted functions expect (normally declared elsewhere in the page).
    tabs: [], activeTabId: null, tabSeq: 0, tabTitleSeq: 0,
    request_num: 0, params_restore_pending_token: -1,
    controller: null, multiQueryControllers: [],
    save_timer: null, column_color_modes: {},
    /// With `run=1` propagation enabled, the test can assert that refreshing an entry
    /// from an unrun draft drops the auto-run marker.
    run_immediately: true,
    user_elem: { value: '' },
    query_area: { value: '', focus() {} },
    document: { title: '', documentElement: { style: { setProperty() {} } } },
    location: { origin: 'http://localhost:8123', pathname: '/play', href: 'http://localhost:8123/play' },
    /// The live parameter inputs, keyed by name; `getParamValues` snapshots them like the
    /// real one reads the `param_*` DOM inputs.
    param_inputs: {},
    getParamValues: () => ({ ...sandbox.param_inputs }),
    endFlight: () => {},
    renderTabBar: () => {},
    queryToColor: () => '',
    persist: async () => {},
    /// Mimic the editor-facing contract of the real `restoreFromHistory`: set the editor
    /// and the parameter inputs to the state's query/params (the real one resets every
    /// detected param, absent -> ''); the synthetic 'input' event it dispatches syncs the
    /// active tab, and the restored param snapshot is captured into it.
    restoreFromHistory: async state =>
    {
        sandbox.query_area.value = state.query;
        sandbox.param_inputs = { ...(state.params || {}) };
        const tab = sandbox.tabs.find(t => t.id === sandbox.activeTabId);
        if (tab) { tab.query = sandbox.query_area.value; tab.params = sandbox.getParamValues(); }
    },
};
sandbox.url_elem = { value: sandbox.location.origin };
/// `new URL(window.location)` (persistColorModes) coerces the location to its href.
sandbox.location.toString = function() { return this.href; };
sandbox.window = sandbox;
/// Browser-history stub: a stack of entries; Back/Forward fire `onpopstate` with the
/// state of the entry navigated to, exactly like a browser. Navigating (push/replace/
/// back/forward) also syncs `location.href` to the current entry's URL, as a real browser
/// does — persistColorModes and the onpopstate restamp read `window.location` back.
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

/// Persistence + startup-reconciliation surface, so the reload path (persist -> reload ->
/// reconcileStartup) can be driven for real. The reconciliation functions that touch the DOM
/// tokenizer / auto-run are stubbed; the draft-vs-URL decision itself runs from the real code.
sandbox.TAB_STORE = 'tabs';
sandbox.META_STORE = 'meta';
sandbox.currentQueryParams = [];
sandbox.bootstrap_dirty = false;
sandbox.bootstrap_settled = false;
sandbox.has_url_query = false;
sandbox.url_tab_name = null;
sandbox.url_query = '';
sandbox.current_url = new URL(sandbox.location.href);
sandbox.defer_run_for_reconcile = false;
sandbox.postAllCalled = false;
sandbox.updateQueryParams = async () => true;
sandbox.setParamValues = values => { if (values) for (const [k, v] of Object.entries(values)) sandbox.param_inputs[k] = v; };
/// The auto-run on a still-authoritative URL. The reload cases assert this fires for a clean
/// run but never for a restored unrun draft.
sandbox.postAll = async () => { sandbox.postAllCalled = true; };
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
        /// Resolve the read/write transaction after the synchronous calls, like the real API.
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

/// A keystroke reaches the page as a DOM edit plus the 'input' listener syncing the
/// active tab in memory; browser history and the URL are deliberately not touched.
function type(q)
{
    sandbox.query_area.value = q;
    const tab = active();
    if (tab) tab.query = q;
}

/// A trusted edit of a parameter input: the real `param-` input handler persists the
/// values into the active tab and stamps the current history entry via `syncHistory`.
function setParam(name, value)
{
    sandbox.param_inputs[name] = value;
    const tab = active();
    if (tab) { tab.params = sandbox.getParamValues(); sandbox.syncHistory(); }
}

/// A successful run: the editor holds the query and `saveHistory` records the result
/// snapshot and the history entry, as the query-execution path does.
async function run(q)
{
    type(q);
    sandbox.saveHistory({ query: q, resultQuery: q, params: sandbox.getParamValues(), format: 'JSONCompact', ok: true, data: 'result of ' + q, elapsed_ns: 1 });
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
    sandbox.param_inputs = {};
    sandbox.document.title = '';
    /// Simulate a session opened from a `run=1` URL so a genuine run stamps `run=1` into its entry
    /// and the reload cases can observe that a restored unrun draft has it dropped. A real reload
    /// starts a fresh JS context; here `reconcileStartup` mutates this global, so restore it.
    sandbox.run_immediately = true;
    const tab = sandbox.makeTab();
    sandbox.tabs.push(tab);
    sandbox.activeTabId = tab.id;
}

/// Simulate a full page reload of the current history entry. The debounced workspace save is
/// forced to fire (the sandbox `setTimeout` never does) by calling `persist` explicitly — as a
/// real >400 ms wait would — then `reconcileStartup` reads the saved workspace back from the fake
/// IndexedDB and reconciles it against the URL, exactly the path a real reload takes. The URL the
/// browser reloads is the current history entry's; a fresh document has the editor seeded from the
/// hash and empty parameter inputs. `postAllCalled` records whether the restored query was auto-run.
async function reload()
{
    await sandbox.persist();
    const cur = sandbox.history.stack[sandbox.history.idx];
    sandbox.current_url = new URL(cur.url, sandbox.location.origin);
    sandbox.url_query = (cur.state && cur.state.query) || '';
    sandbox.url_tab_name = sandbox.current_url.searchParams.get('tab');
    sandbox.has_url_query = sandbox.url_query.length > 0;
    sandbox.run_immediately = sandbox.current_url.searchParams.has('run');
    sandbox.defer_run_for_reconcile = sandbox.run_immediately && sandbox.has_url_query;
    sandbox.query_area.value = sandbox.url_query;
    sandbox.param_inputs = {};
    sandbox.bootstrap_dirty = false;
    sandbox.bootstrap_settled = false;
    sandbox.postAllCalled = false;
    sandbox.request_num = 0;
    sandbox.save_timer = null;
    await sandbox.reconcileStartup();
    await drain();
}

(async () =>
{
    /// Closing the active tab folds its latest draft into its history entry,
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

    /// A same-session Back-then-Forward round-trip preserves a newer unrun draft
    /// instead of restoring the entries' older queries over it.
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

    /// The preserved draft carries its parameter bindings too: Back must not restore the
    /// older entry's params under the newer draft query — that would silently drop edited
    /// values and let the scheduled save persist an incoherent query/params pair.
    reset();
    await run('SELECT 0');
    type('SELECT {x:Int32}');
    setParam('x', '1');
    await run('SELECT {x:Int32}');
    type('SELECT {x:Int32} + {y:Int32}');
    setParam('y', '2');
    sandbox.history.back();
    await drain();
    assert_eq('param back: the draft query is preserved', active().query, 'SELECT {x:Int32} + {y:Int32}');
    assert_params('param back: the draft params are preserved', active().params, { x: '1', y: '2' });
    assert_params('param back: the param inputs keep the draft bindings', sandbox.param_inputs, { x: '1', y: '2' });
    sandbox.history.forward();
    await drain();
    assert_eq('param forward: the draft query survives the round-trip', active().query, 'SELECT {x:Int32} + {y:Int32}');
    assert_params('param forward: the draft params survive the round-trip', active().params, { x: '1', y: '2' });

    /// Control: with a clean editor (the tab still reflects its run), entry params keep
    /// restoring verbatim, including dropping bindings the older entry does not carry.
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

    /// Reload after a preserved-draft Back/Forward round-trip. The debounced save persists the
    /// draft (query != lastSavedQuery, the shape reconcileStartup treats as a stale reload), so
    /// the reload restores the draft as editor text — but the stale-reload branch drops the URL's
    /// run=1, so the unrun draft is NOT auto-executed. This is the path the earlier harness could
    /// not reach, since its `setTimeout` stub never let `persist` run.
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

    /// Reload after a color-mode toggle over an unrun draft. persistColorModes is a result-only
    /// change, but its debounced save snapshots the live editor (`captureActiveTab`), so the draft
    /// is persisted and restored on reload — again only as editor text, never auto-run.
    reset();
    await run('SELECT 1');
    type('SELECT 2');
    sandbox.column_color_modes = { c: 'heatmap' };
    sandbox.persistColorModes();
    await reload();
    sandbox.column_color_modes = {};
    assert_eq('reload after a color toggle over a draft: the draft is restored as editor text', active().query, 'SELECT 2');
    assert_eq('reload after a color toggle over a draft: the draft is not auto-run', sandbox.postAllCalled, false);

    /// Control: reloading a clean run (no draft) restores the run's query AND re-runs it — the URL
    /// is still authoritative (run=1 preserved) — which is the "reload re-runs what you ran"
    /// behavior the draft cases above deliberately do not trigger.
    reset();
    await run('SELECT 0');
    await run('SELECT 1');
    await reload();
    assert_eq('reload of a clean run: the run query is restored', active().query, 'SELECT 1');
    assert_eq('reload of a clean run: the run query is auto-run', sandbox.postAllCalled, true);

    console.log('OK');
})().catch(e => { console.error('FAIL: ' + (e && e.stack || e)); process.exit(1); });
EOF

node "$harness" "$html"
rm -f "$html" "$harness"
