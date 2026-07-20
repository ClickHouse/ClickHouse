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
#     (no draft) keeps restoring entries verbatim; "draft" means dirty since the tab's
#     last history write (`tab.query !== tab.lastSavedQuery`), NOT `!tabReflectsRun`,
#     so a clean `Run selected` (whose result snapshots only the selected statement) is
#     not mistaken for a draft and Back/Forward still restore its entry queries;
#   - the preserved draft carries its parameter bindings too: Back must not restore the
#     older entry's params under the newer draft query (with a clean editor, entry params
#     keep restoring verbatim);
#   - the reload path (persist -> reconcileStartup): once any debounced save flushes an
#     unrun draft that diverges from the URL (the onpopstate save after a preserved-draft
#     Back/Forward, or persistColorModes over a draft), a reload restores that draft as
#     editor text but NEVER auto-runs it (the stale-reload branch refuses the URL's run=1);
#     a clean run, by contrast, is both restored and re-run on reload. The suppression is
#     scoped to that draft, not the session: a later genuine full run still writes a run=1
#     entry (and reloading it re-runs it), and switching to another, still clean run-backed
#     tab after the stale reload rewrites that tab's URL with run=1 intact;
#   - typing does not cancel an in-flight run, so a delayed completion must not clobber a
#     newer, unrun draft (or its live parameter edits) typed while the run was still in
#     flight: `saveHistory` must leave the live editor/params alone and drop `run=1` on the
#     entry it produces, even though it still keeps the completed run's own result snapshot.
#     Pinned for both a single query and a "Run all" of several, the latter driving the real
#     `postMulti` (not just `saveHistory`) so a regression in its own launch-time snapshot
#     (`launch_query_text`) is caught too, not only one in `saveHistory`'s guard. When the
#     in-flight edit CHANGED the placeholder set, the newer draft's entry must record ITS OWN
#     parameters, derived from the draft query TEXT (`paramValuesForQuery`) — even if the edit's
#     async `updateQueryParams` rebuild has not landed by the time the run completes, so the live
#     inputs still describe the previous query — never the stale binding `getParamValues()` would
#     still read (which would leak e.g. `param_x=1` into the `SELECT {y:Int32}` entry / URL);
#   - the run entrypoints snapshot the launched query AND caret/selection range BEFORE the WASM-lexer
#     await, so a draft typed (or a caret moved) while the lexer loads never becomes what actually
#     runs: `postAll`'s single-statement branch runs the parsed launch text rather than re-reading
#     the live editor (which on the `run=1` path would auto-run a never-launched draft), `postOne`'s
#     `Run selected` picks statements from the launch-time selection, not the moved caret, and the
#     no-selection `Run one` path (real `getQueryUnderCursor`) picks the statement under the caret at
#     launch, not wherever the caret is moved to before the lexer resolves;
#   - a run that did not execute the WHOLE editor is never recorded as auto-runnable (`run=1`): its
#     history text is the full editor, so a reload would `postAll` and run statements that never
#     launched. This covers a "Run selected" / "Run one" subset AND a full "Run all" that stops on an
#     error partway (every statement after the failing one is skipped) — the latter must drop `run=1`
#     just like a subset, so a shared link / reload never auto-executes a never-run (possibly
#     destructive) trailing statement.
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
    'buildHistoryParams', 'isCurrentEntryForTab', 'writeHistoryEntry', 'sameParams', 'tabReflectsRun',
    'refreshCurrentHistoryEntry', 'saveHistory', 'syncHistory', 'resolveTabForState',
    /// The connection/database identity helpers the merged history writers and Back/Forward restore
    /// now consult (added on master). Extracted so the run=1 / draft cases run the REAL divergence
    /// and effective-database logic: a run-produced entry stamps the live connection and stays
    /// reproducible (`run=1` kept), and the selected database (always null / server default in these
    /// cases) never spuriously drops the marker. `applySelectedDatabaseHighlight` is DOM-only and is
    /// stubbed below.
    'liveDivergedFromRun', 'effectiveDatabase', 'sameServerAddress', 'effectiveConnectionUser',
    'stampSelectedDatabaseConnection',
    /// The real structural tab operations, so the draft-in-another-tab case below drives the
    /// same capture/refresh/activate/sync sequence a tab click or the "+" button does.
    'markBootstrapDirty', 'switchToTab', 'addTab',
    /// The persistence + startup-reconciliation surface exercised by the reload cases below.
    'loadFromDb', 'persist', 'persistColorModes', 'reconcileStartup',
    /// The real multi-query completion path, so the delayed-completion case below actually
    /// exercises `postMulti`'s own use of its launch-time `launch_query_text` argument instead
    /// of re-deriving a snapshot the same way `saveHistory` does -- a regression in how `postAll`/
    /// `postOne` capture and thread that argument through would go uncaught otherwise.
    'postMulti',
    /// The real single-query entrypoint and completion path, so the in-flight single-run case below
    /// pins `postOne`'s OWN launch-time snapshot of the editor -- the `history_query_text` it threads
    /// into `postSingle`. A regression that re-read `query_area.value` after the tokenization await
    /// would stamp a draft typed meanwhile as `run=1`, and neither `saveHistory` nor `postMulti`
    /// exercises that path.
    ///
    /// The real parameter resolver, so the params-restore-pending run case below drives `postSingle`'s
    /// real sourcing of the destination tab's own saved params (rather than the stale live inputs)
    /// end to end. For `params_restore_pending = false` it just returns `getParamValues()`, exactly
    /// what the previous stub did, so the other cases are unaffected. When the destination query was
    /// edited before Run, it derives the launched query's own parameters from its TEXT via
    /// `paramValuesForQuery` (which tokenizes and enumerates the placeholders with `extractQueryParams`)
    /// rather than the possibly-stale live inputs, so the edited-query race case below runs the real
    /// derivation instead of a stub.
    'resolveRunParams', 'paramValuesForQuery', 'extractQueryParams',
    'postSingle', 'postOne'];
let code = FUNCS.map(f => extractTopLevel(new RegExp('^(async )?function ' + f + '\\('), f)).join('\n');
code += '\n' + extractTopLevel(/^window\.onpopstate = /, 'window.onpopstate');
/// The real "Run all" / `run=1` auto-run entrypoint, extracted under an alias so the sandbox's
/// `postAll` stays the `reconcileStartup` stub the reload cases assert on (`postAllCalled`). The
/// in-flight "Run all" single-statement case below drives this to pin `postAll`'s launch-time
/// editor snapshot: it must run the statement parsed from the launch-time text, never re-read
/// `query_area.value` after the `splitAllQueries` lexer await.
code += '\n' + extractTopLevel(/^async function postAll\(/, 'postAll').replace('async function postAll(', 'async function realPostAll(');
/// The real "Run one" statement selector, extracted under an alias so the sandbox's
/// `getQueryUnderCursor` stays the reconcileStartup/no-op stub the other cases rely on. The
/// no-selection in-flight case below drives this to pin its launch-time caret snapshot: it must
/// choose the statement the caret was in when `Run` was pressed, never re-read `selectionStart`
/// after its own `tokenize` (WASM-lexer) await.
code += '\n' + extractTopLevel(/^async function getQueryUnderCursor\(/, 'getQueryUnderCursor').replace('async function getQueryUnderCursor(', 'async function realGetQueryUnderCursor(');

/// Queued resolvers for in-flight `postImpl` calls made by the real `postMulti` under test
/// (see `startMultiRun`/`finishMultiRun`); each one hangs until `resolvePendingPostImpl` below
/// releases it, so a test can type a draft in the gap between launching a multi-query run and
/// its completion, exactly like a real network round-trip that outlives a keystroke.
let pendingPostImpl = [];
function resolvePendingPostImpl()
{
    const pending = pendingPostImpl;
    pendingPostImpl = [];
    for (const resolve of pending) resolve();
}

const sandbox = {
    console, URL, TextEncoder, TextDecoder, AbortController, performance,
    btoa: s => Buffer.from(s, 'binary').toString('base64'),
    atob: s => Buffer.from(s, 'base64').toString('binary'),
    /// `scheduleSave` must arm its debounce timer without the save ever firing,
    /// so the run stays deterministic and node exits without pending timers.
    setTimeout: () => 1,
    /// Globals the extracted functions expect (normally declared elsewhere in the page).
    tabs: [], activeTabId: null, tabSeq: 0, tabTitleSeq: 0,
    request_num: 0, params_restore_pending_token: -1, params_restore_pending_query: null,
    controller: null, multiQueryControllers: [], multiQueryContainer: null,
    save_timer: null, column_color_modes: {}, elapsed_ns: 0, last_query_start: 0,
    /// The shared display-state globals for pinned columns / per-column color modes and their
    /// `?pinned_columns=` / `?color_modes=` URL carriers (added on master): no case here opens
    /// such a link, so the URL carriers stay absent (null / not-malformed) and the pinned set
    /// stays empty, exactly like a plain `/play` load.
    pinned_columns: Object.create(null), url_pinned_columns: null, pinned_columns_url_malformed: false,
    url_color_modes: null, color_modes_url_malformed: false,
    /// With `run=1` propagation enabled, the test can assert that refreshing an entry
    /// from an unrun draft drops the auto-run marker. `url_run_directive` is the immutable
    /// "the URL carried `?run=1`" fact; entry stamping keys off it plus the per-entry
    /// `fromRun`, and `run_directive_spent` (set when reconciliation drops the directive's
    /// target context — pruned blank tab / dirty-startup merge) stays false in these cases.
    url_run_directive: true,
    run_directive_spent: false,
    user_elem: { value: '' },
    /// `selectionStart`/`selectionEnd` back the `Run selected` path; `has_selection` and the
    /// selected statements are derived from them (the in-flight selection case moves them mid-run).
    query_area: { value: '', selectionStart: 0, selectionEnd: 0, focus() {}, setSelectionRange(s, e) { this.selectionStart = s; this.selectionEnd = e; } },
    document: {
        title: '', documentElement: { style: { setProperty() {} } },
        /// `postMulti` only ever toggles `.style.display` on these and creates plain
        /// containers / `query-result` elements for its per-statement progress listeners.
        /// A `param-<name>` id, however, is the live parameter input the real `paramValuesForQuery`
        /// reads by name (`resolveRunParams` now derives every run's params from the launched query
        /// text, not the possibly-stale `currentQueryParams`): map it to `param_inputs`, returning an
        /// element with the input's `.value` when the input exists, else `null` — exactly the
        /// "input already there / not yet rebuilt" distinction the real DOM draws.
        getElementById: id =>
        {
            if (id && id.startsWith('param-'))
            {
                const name = id.slice('param-'.length);
                return Object.prototype.hasOwnProperty.call(sandbox.param_inputs, name)
                    ? { value: sandbox.param_inputs[name] }
                    : null;
            }
            return { style: {} };
        },
        createElement: tag => tag === 'query-result'
            ? { style: {}, rowCount: 0, elapsedNs: 0, incompleteResult: false, addEventListener() {} }
            : { style: {}, innerHTML: '', appendChild() {} },
        /// `reconcileStartup` re-lays-out every rendered `query-result` for the reasserted
        /// pinned-column / color-mode display state; nothing is rendered in this sandbox.
        querySelectorAll: () => [],
    },
    location: { origin: 'http://localhost:8123', pathname: '/play', href: 'http://localhost:8123/play' },
    /// `postMulti`'s own rendering; the tab/history bookkeeping it drives is the real code.
    resultEl: { style: {}, parentElement: { insertBefore() {} } },
    progressEl: { start() {}, finish() {}, clear() {}, updateText() {}, updateProgress() {}, style: { setProperty() {} } },
    logoEl: { style: {} },
    clear: () => {},
    /// Stands in for the network round-trip: hangs until `resolvePendingPostImpl` above
    /// releases it, always reporting a successful, non-image, non-raw text result.
    postImpl: (posted_request_num, query) => new Promise(resolve => pendingPostImpl.push(() => resolve({
        cancelled: false, is_error: false, response_ok: true, format: 'JSONCompact',
        reply: 'result of ' + query, is_table: false, is_raw: false, is_chart: false, is_image: false,
    }))),
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
/// The connection password input the merged `postSingle`/`postMulti` snapshot into the per-run
/// connection tuple (never persisted). Empty for the default same-origin connection used here.
sandbox.password_elem = { value: '' };
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
/// The WASM lexer. `resolveRunParams` -> `paramValuesForQuery` tokenizes every run's launched query
/// to enumerate its own placeholders (rather than trusting the possibly-stale live inputs), so a
/// resolving default is needed even for the param-less runs; cases that exercise a specific query's
/// placeholders (or a mid-await caret/edit race) override it with their own token stream.
sandbox.tokenize = async () => [];
sandbox.setParamValues = values => { if (values) for (const [k, v] of Object.entries(values)) sandbox.param_inputs[k] = v; };
/// Connection/database state the merged history writers + Back/Forward restore read (added on
/// master). These cases never select a database or change the connection, so the selection stays
/// null (the server default) and the live connection matches the producing one on every run — so
/// `liveDivergedFromRun` is false and `run=1` is preserved exactly as before the merge. The
/// server's current database is unknown to this harness (no `system.databases` fetch), which
/// `effectiveDatabase` treats as "no canonical default", leaving null === null. `deferred_run_cancelled`
/// is the startup auto-run kill switch the editor-only writers set; irrelevant post-startup here.
sandbox.selected_database = null;
sandbox.selected_database_connection = null;
sandbox.server_current_database = null;
sandbox.deferred_run_cancelled = false;
/// DOM-only: highlights the selected row in the databases panel. No panel in the harness, so the
/// real one would throw on `querySelectorAll`; the highlight is irrelevant to the history contract.
sandbox.applySelectedDatabaseHighlight = () => {};
/// The auto-run on a still-authoritative URL. The reload cases assert this fires for a clean
/// run but never for a restored unrun draft.
sandbox.postAll = async () => { sandbox.postAllCalled = true; };
/// Globals the real `postOne`/`postSingle` read that are defined elsewhere in the page. The
/// in-flight single-run case overrides `getQueryUnderCursor` to hang on its (WASM-lexer) await so a
/// draft can be typed mid-run; `splitAllQueries` is only reached by a selection run, not driven here.
sandbox.beginFlight = () => {};
sandbox.isMultiQuery = false;
sandbox.queryUnderCursorStart = 0;
/// Set by the real `getQueryUnderCursor` (whether it ran only one of several statements) and read
/// by `postOne` to decide whether a no-selection "Run one" is a partial run; a page-level global.
sandbox.queryUnderCursorIsPartial = false;
sandbox.last_query_for_download = '';
sandbox.last_params_for_download = {};
sandbox.getQueryUnderCursor = async () => '';
/// Only the real `getQueryUnderCursor` (driven by the no-selection in-flight case below) reads
/// these: `getQueryBoundaries` gates whether it visually re-selects the chosen statement (a length
/// > 1 means "select it"), and `focusEditorForRun` focuses the editor. Neither affects WHICH
/// statement it returns, so both are stubbed while the caret-vs-await logic under test runs for real.
sandbox.getQueryBoundaries = () => [{}, {}];
sandbox.focusEditorForRun = () => {};
sandbox.splitAllQueries = async text => text.split(';').map(q => ({ query: q.trim(), is_select: true, start: 0, end: 0 }));
sandbox.document.body = { scrollTo() {} };
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
    await sandbox.saveHistory({ query: q, resultQuery: q, params: sandbox.getParamValues(), format: 'JSONCompact', ok: true, data: 'result of ' + q, elapsed_ns: 1,
        /// Stamp the live connection the run executed against, exactly as `postSingle`/`postMulti` do,
        /// so the snapshot is recognized as reproducible on the current connection (`liveDivergedFromRun`
        /// false) and the entry keeps `run=1`; an unstamped snapshot is treated as diverged (fail closed).
        database: sandbox.selected_database, url: sandbox.url_elem.value, user: sandbox.user_elem.value });
    await drain();
}

/// A "Run selected" execution: the editor holds the full text, but only the selected
/// statement produced the result, so `saveHistory` stores the full editor in `tab.query`
/// and the selected statement in `tab.result.query` — exactly as the real page does.
async function runSelected(editorText, selectedStatement)
{
    type(editorText);
    await sandbox.saveHistory({ query: editorText, resultQuery: selectedStatement, params: sandbox.getParamValues(), format: 'JSONCompact', ok: true, data: 'result of ' + selectedStatement, elapsed_ns: 1,
        database: sandbox.selected_database, url: sandbox.url_elem.value, user: sandbox.user_elem.value });
    await drain();
}

/// Begin a run without completing it, capturing the query/params at launch time — exactly what
/// `postSingle`'s `history_query_text`/`postMulti`'s `launch_query_text` and `resolveRunParams`
/// snapshot before the network round-trip. Pair with `finishRun` once something else has
/// happened to the live tab/params meanwhile (typing does not cancel an in-flight request).
function startRun(q)
{
    type(q);
    return { query: q, params: sandbox.getParamValues() };
}

/// Complete a run started with `startRun`: `saveHistory` receives the LAUNCH-TIME snapshot,
/// never whatever the editor/params hold by the time the response actually arrives.
async function finishRun(started)
{
    await sandbox.saveHistory({ query: started.query, resultQuery: started.query, params: started.params, format: 'JSONCompact', ok: true, data: 'result of ' + started.query, elapsed_ns: 1,
        database: sandbox.selected_database, url: sandbox.url_elem.value, user: sandbox.user_elem.value });
    await drain();
}

/// Like `startRun`, but drives the REAL `postMulti` (not a `saveHistory` snapshot re-created by
/// the test) with two SELECTs, so the `postImpl` calls it makes hang until `finishMultiRun`
/// resolves them below. This is what actually pins `postMulti`'s own `launch_query_text` handling:
/// a regression that made it re-read `query_area.value` instead would go uncaught by `startRun`/
/// `finishRun`, which never call `postMulti` at all.
function startMultiRun(editorText)
{
    type(editorText);
    const parsed = editorText.split(';').map(query => ({ query: query.trim(), is_select: true }));
    return sandbox.postMulti(sandbox.request_num, parsed, false, editorText);
}

/// Complete a run started with `startMultiRun`: release its `postImpl` calls and await `postMulti`
/// itself before letting the caller inspect the tab/history it produced.
async function finishMultiRun(promise)
{
    /// `postMulti` yields a microtask (`await resolveRunParams`) before it registers its hung
    /// `postImpl` calls, so let it reach them first — otherwise `resolvePendingPostImpl` runs
    /// against an empty queue and the run hangs waiting for resolvers that were never released.
    await drain();
    resolvePendingPostImpl();
    await promise;
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
    sandbox.document.title = '';
    /// Simulate a session opened from a `run=1` URL so a genuine run stamps `run=1` into its entry
    /// and the reload cases can observe that a restored unrun draft has it dropped. A real reload
    /// starts a fresh JS context; here `reload` re-derives this global from the reloaded URL,
    /// so restore it (and the spent marker with it).
    sandbox.url_run_directive = true;
    sandbox.run_directive_spent = false;
    /// Clear the session-accumulated deferred-run cancellation between cases, so a prior case's
    /// editor-only write does not leak a cancelled auto-run into the next case's reload.
    sandbox.deferred_run_cancelled = false;
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
    sandbox.url_run_directive = sandbox.current_url.searchParams.has('run');
    sandbox.run_directive_spent = false;
    sandbox.defer_run_for_reconcile = sandbox.url_run_directive;
    /// A real reload is a fresh JS context, so the session-accumulated "the deferred startup auto-run
    /// was cancelled" flag starts false (module init) — reconcileStartup gates the auto-run on it.
    sandbox.deferred_run_cancelled = false;
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

    /// Same close-then-Back fold, but when the current entry is a LEGACY pre-`tabId` state (only a
    /// `tabName`, as `resolveTabForState`/`onpopstate` still support). The structural fold must adopt
    /// that entry as the active tab (`isCurrentEntryForTab` matches it by title) so it captures the
    /// draft and upgrades it to a `tabId` entry — otherwise the fold bails and Back restores the
    /// stale last-run query instead of the draft.
    reset();
    await run('SELECT 1');
    /// Rewrite the current entry into a legacy shape: keep only `query`/`tabName`, drop `tabId`.
    const legacy_entry = sandbox.history.stack[sandbox.history.idx];
    legacy_entry.state = { query: 'SELECT 1', params: {}, result: legacy_entry.state.result, tabName: active().title };
    type('SELECT 1 -- draft');
    sandbox.closeTab(sandbox.activeTabId);
    await drain();
    assert_eq('legacy close: the legacy entry captures the latest draft', sandbox.history.stack[0].state.query, 'SELECT 1 -- draft');
    assert_eq('legacy close: the folded entry is upgraded to a tabId entry', !!sandbox.history.stack[0].state.tabId, true);
    sandbox.history.back();
    await drain();
    assert_eq('legacy close+back: the recreated tab restores the draft', active().query, 'SELECT 1 -- draft');

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
    /// values and let the scheduled save persist an incoherent query/params pair. A parameter
    /// edit calls `syncHistory`, which stamps the current query + params into a history entry
    /// (so a param-edited draft is itself saved, not at-risk); a keystroke AFTER it makes the
    /// editor dirty again (`tab.query !== tab.lastSavedQuery`), so this draft — with its live
    /// bindings — is genuinely preserved across the round-trip.
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

    /// A clean `Run selected` is NOT a draft. Its result snapshots only the selected statement,
    /// so `tabReflectsRun` is false while the editor keeps the full text, but the editor was not
    /// edited after the run. Back/Forward must restore the older / full-editor entry query, not
    /// mistake the run-backed editor for a newer unrun draft. The preserve check keys off
    /// `tab.query !== tab.lastSavedQuery` ("dirty since this entry was written"), which stays false
    /// here, so navigation restores the entry queries verbatim.
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

    /// A run's completion must not clobber a newer, unrun draft typed while it was in flight:
    /// typing does not cancel a request, so by the time it resolves the editor may already hold
    /// text that has nothing to do with what actually ran (`saveHistory`'s `ranLiveQuery` guard).
    /// The completed run's own result snapshot is still kept — rendering/downloads must reflect
    /// what actually ran — but the live draft survives and the entry it produces carries no
    /// `run=1` (that draft itself was never run).
    reset();
    await run('SELECT 0');
    const inFlight = startRun('SELECT 1');
    type('SELECT 2 -- typed while SELECT 1 was still in flight');
    await finishRun(inFlight);
    assert_eq('delayed completion: the live draft survives', active().query, 'SELECT 2 -- typed while SELECT 1 was still in flight');
    assert_eq("delayed completion: the completed run's result is still kept", active().result && active().result.query, 'SELECT 1');
    assert_eq('delayed completion: the entry does not carry run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);

    /// Same hazard when only the PARAMETERS changed while the query itself did not (e.g. a param
    /// widget edited during an in-flight run): the live parameter edit must survive, and must not
    /// be silently paired with the completed run's own (now stale) parameter values under `run=1`.
    reset();
    await run('SELECT 0');
    const inFlightParams = startRun('SELECT {x:Int32}');
    setParam('x', '9');
    await finishRun(inFlightParams);
    assert_eq('delayed completion (params changed): the query is kept', active().query, 'SELECT {x:Int32}');
    assert_params('delayed completion (params changed): the live param edit survives', active().params, { x: '9' });
    assert_eq('delayed completion (params changed): the entry does not carry run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);

    /// Same hazard again, but through the REAL `postMulti` (a "Run all" of more than one
    /// statement) instead of a `saveHistory` snapshot the test re-creates by hand: this is what
    /// pins `postMulti`'s own `launch_query_text` argument (see `startMultiRun`/`finishMultiRun`).
    /// A regression that made `postMulti` re-read `query_area.value` at completion time instead
    /// of using its launch-time argument would stamp the live draft below as `run=1`.
    reset();
    await run('SELECT 0');
    const inFlightMulti = startMultiRun('SELECT 1; SELECT 2');
    type('SELECT 3 -- typed while the multi-query run was still in flight');
    await finishMultiRun(inFlightMulti);
    assert_eq('delayed completion (multi-query): the live draft survives', active().query, 'SELECT 3 -- typed while the multi-query run was still in flight');
    assert_eq("delayed completion (multi-query): the completed run's result is still kept", active().result && active().result.multi && active().result.multi.length, 2);
    assert_eq('delayed completion (multi-query): the entry does not carry run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);

    /// A single-query run with the caret in one of several statements. `postOne` must await the WASM
    /// lexer (`getQueryUnderCursor`) before it knows which statement to run, and typing during that
    /// await does not cancel the run. The launched statement (`SELECT 1`) is what actually executes,
    /// so the history entry must never stamp the draft typed meanwhile (`SELECT 99; ...`) as `run=1`
    /// -- a reload / shared link would auto-execute text that never ran. This drives the REAL
    /// `postOne`/`postSingle`: a regression that re-read `query_area.value` after the await instead of
    /// the launch-time snapshot would mark the draft below `run=1`.
    reset();
    sandbox.isMultiQuery = true;
    type('SELECT 1; SELECT 2');
    let releaseCursor;
    sandbox.getQueryUnderCursor = () => new Promise(resolve => { releaseCursor = () => resolve('SELECT 1'); });
    const onePromise = sandbox.postOne();
    await drain();
    type('SELECT 99; SELECT 2');   /// draft typed while the lexer is still loading
    releaseCursor();
    await drain();
    resolvePendingPostImpl();
    await onePromise;
    await drain();
    assert_eq('in-flight single run: the never-run draft is not stamped run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);
    assert_eq('in-flight single run: the result snapshot is the statement that actually ran', active().result && active().result.query, 'SELECT 1');
    assert_eq('in-flight single run: the live draft survives in the editor', active().query, 'SELECT 99; SELECT 2');
    sandbox.isMultiQuery = false;

    /// `Run selected` must target the statement selected when the run was LAUNCHED, even if the
    /// caret moves (or text is typed) while the `splitAllQueries` lexer promise is still in flight --
    /// typing/moving does not cancel the run. `postOne` snapshots the selection range together with
    /// the text before that await; a regression that re-read `query_area.selectionStart`/`selectionEnd`
    /// afterwards would run whichever statement the moved caret now overlaps (`SELECT 1`) instead of
    /// the launched one (`SELECT 2`). Drives the REAL `postOne`/`postSingle` with a hung lexer.
    reset();
    sandbox.isMultiQuery = true;
    sandbox.query_area.value = 'SELECT 1; SELECT 2';
    active().query = 'SELECT 1; SELECT 2';
    sandbox.query_area.selectionStart = 10;   /// 'SELECT 2' selected at launch time
    sandbox.query_area.selectionEnd = 18;
    let releaseSelSplit;
    sandbox.splitAllQueries = () => new Promise(resolve => { releaseSelSplit = () => resolve([
        { query: 'SELECT 1', is_select: true, start: 0, end: 8, queryStart: 0 },
        { query: 'SELECT 2', is_select: true, start: 10, end: 18, queryStart: 10 },
    ]); });
    const selPromise = sandbox.postOne();
    await drain();
    sandbox.query_area.selectionStart = 1;    /// caret moved near the start while the lexer loads
    sandbox.query_area.selectionEnd = 1;
    releaseSelSplit();
    await drain();
    resolvePendingPostImpl();
    await selPromise;
    await drain();
    assert_eq('run-selected in-flight: the launched selection runs, not the moved caret', active().result && active().result.query, 'SELECT 2');
    sandbox.isMultiQuery = false;

    /// A "Run all" of a SINGLE statement on the `run=1` / reload path must run the statement parsed
    /// from the launch-time text, never re-read the live editor after the `splitAllQueries` lexer
    /// await. Drives the REAL `postAll` (aliased to `realPostAll`, see above): launch `SELECT 1`, type
    /// `DROP TABLE important` while the lexer is still loading, and the run/result/history stay on
    /// `SELECT 1` while the live draft survives untouched and unrun. A regression that re-read
    /// `query_area.value` (via `getQueryUnderCursor`) would execute the draft and stamp it `run=1`,
    /// so a shared link / reload would auto-run text that was never launched.
    reset();
    sandbox.query_area.value = 'SELECT 1';
    active().query = 'SELECT 1';
    /// The regressed branch reads the live editor here; the fixed one uses the parsed launch text.
    sandbox.getQueryUnderCursor = async () => sandbox.query_area.value;
    let releaseAllSplit;
    sandbox.splitAllQueries = text => new Promise(resolve => { releaseAllSplit = () => resolve([
        { query: text.trim(), is_select: true, start: 0, end: text.length, queryStart: 0 },
    ]); });
    const allPromise = sandbox.realPostAll();
    await drain();
    type('DROP TABLE important');   /// draft typed while the lexer is still loading
    releaseAllSplit();
    await drain();
    resolvePendingPostImpl();
    await allPromise;
    await drain();
    assert_eq('in-flight run-all single: the launched statement runs, not the draft', active().result && active().result.query, 'SELECT 1');
    assert_eq('in-flight run-all single: the never-run draft is not stamped run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);
    assert_eq('in-flight run-all single: the live draft survives in the editor', active().query, 'DROP TABLE important');

    /// `Run one` with NO selection must run the statement the caret was in WHEN THE RUN WAS LAUNCHED,
    /// even if the caret moves while `getQueryUnderCursor` awaits the WASM lexer -- moving it does not
    /// cancel the run. This drives the REAL `getQueryUnderCursor` (extracted as `realGetQueryUnderCursor`,
    /// normally stubbed) with a hung `tokenize`: the caret starts inside `SELECT 1`, moves into
    /// `SELECT 2` mid-await, and the launched statement (`SELECT 1;`) must still be what runs. A
    /// regression that read `query_area.selectionStart` after the await would run `SELECT 2` from the
    /// moved caret -- a statement the user never launched.
    reset();
    sandbox.isMultiQuery = true;
    sandbox.query_area.value = 'SELECT 1; SELECT 2';
    active().query = 'SELECT 1; SELECT 2';
    sandbox.query_area.selectionStart = 3;   /// caret inside `SELECT 1` at launch
    sandbox.query_area.selectionEnd = 3;
    sandbox.getQueryUnderCursor = sandbox.realGetQueryUnderCursor;
    const cursorTokens = [
        { token: 'SELECT', significant: true }, { token: ' ', significant: false },
        { token: '1', significant: true }, { token: ';', significant: true },
        { token: ' ', significant: false }, { token: 'SELECT', significant: true },
        { token: ' ', significant: false }, { token: '2', significant: true },
    ];
    let releaseCursorTokenize;
    /// Hang ONLY the first tokenize -- the one `getQueryUnderCursor` awaits, where the caret race is
    /// under test. Once released, later calls resolve at once; notably the run's own `resolveRunParams`
    /// -> `paramValuesForQuery`, which tokenizes the launched text to enumerate its placeholders, must
    /// not re-hang here (that would deadlock the run this case asserts completes).
    sandbox.tokenize = () => releaseCursorTokenize
        ? Promise.resolve(cursorTokens)
        : new Promise(resolve => { releaseCursorTokenize = () => resolve(cursorTokens); });
    const cursorPromise = sandbox.postOne();
    await drain();
    sandbox.query_area.selectionStart = 13;   /// caret moved into `SELECT 2` while the lexer loads
    sandbox.query_area.selectionEnd = 13;
    releaseCursorTokenize();
    await drain();
    resolvePendingPostImpl();
    await cursorPromise;
    await drain();
    assert_eq('run-one in-flight: the launch-time caret statement runs, not the moved caret', active().result && active().result.query, 'SELECT 1;');
    sandbox.getQueryUnderCursor = async () => '';   /// restore the default stub for the later cases
    sandbox.isMultiQuery = false;

    /// A partial "Run selected" — only ONE of several statements selected and run — must NOT be
    /// recorded as an auto-runnable (`run=1`) entry: its history text is the FULL editor, so a
    /// reload would `postAll` the whole thing and execute statements that never ran (here a
    /// destructive `DROP TABLE`). Drives the REAL `postOne`/`postSingle` (single selected statement)
    /// from the launch-time selection, then reloads and asserts the tail is not auto-run.
    reset();
    sandbox.isMultiQuery = true;
    sandbox.query_area.value = 'SELECT 1; DROP TABLE important';
    active().query = 'SELECT 1; DROP TABLE important';
    sandbox.query_area.selectionStart = 0;     /// 'SELECT 1' selected at launch
    sandbox.query_area.selectionEnd = 8;
    sandbox.splitAllQueries = async () => [
        { query: 'SELECT 1', is_select: true, start: 0, end: 8, queryStart: 0 },
        { query: 'DROP TABLE important', is_select: false, start: 10, end: 30, queryStart: 10 },
    ];
    const selSubsetPromise = sandbox.postOne();
    await drain();
    resolvePendingPostImpl();
    await selSubsetPromise;
    await drain();
    assert_eq('run-selected subset: the entry is not stamped run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);
    assert_eq('run-selected subset: the result snapshot is the statement that ran', active().result && active().result.query, 'SELECT 1');
    sandbox.isMultiQuery = false;
    await reload();
    assert_eq('run-selected subset: a reload does not auto-run the full editor', sandbox.postAllCalled, false);
    assert_eq('run-selected subset: a reload restores the full editor as text', active().query, 'SELECT 1; DROP TABLE important');

    /// A partial "Run selected" of SEVERAL (but not all) statements drives the REAL `postMulti`.
    /// Its result stores the full editor as `tab.result.query` (no single `resultQuery`), so besides
    /// dropping `run=1` at save time, a later tab switch (`syncHistory`) must NOT re-stamp `run=1`
    /// via `tabReflectsRun` — the `partial` flag on the snapshot is what prevents that.
    reset();
    sandbox.isMultiQuery = true;
    sandbox.query_area.value = 'SELECT 1; SELECT 2; DROP TABLE important';
    active().query = 'SELECT 1; SELECT 2; DROP TABLE important';
    sandbox.query_area.selectionStart = 0;     /// 'SELECT 1; SELECT 2' selected at launch
    sandbox.query_area.selectionEnd = 18;
    sandbox.splitAllQueries = async () => [
        { query: 'SELECT 1', is_select: true, start: 0, end: 8, queryStart: 0 },
        { query: 'SELECT 2', is_select: true, start: 10, end: 18, queryStart: 10 },
        { query: 'DROP TABLE important', is_select: false, start: 20, end: 40, queryStart: 20 },
    ];
    const selMultiSubsetPromise = sandbox.postOne();
    await drain();
    resolvePendingPostImpl();
    await selMultiSubsetPromise;
    await drain();
    assert_eq('run-selected multi subset: the entry is not stamped run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);
    sandbox.syncHistory();   /// a tab switch calls this; it must not re-stamp run=1 from the full editor
    assert_eq('run-selected multi subset: a later tab switch does not re-stamp run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);
    sandbox.isMultiQuery = false;
    await reload();
    assert_eq('run-selected multi subset: a reload does not auto-run the full editor', sandbox.postAllCalled, false);

    /// "Run one" (no selection) in multi-query mode runs only the statement under the cursor. When
    /// the editor holds more than that one statement, the run is partial: a `run=1` reload would
    /// `postAll` the whole editor and run the others. Drives the REAL `postOne` + real
    /// `getQueryUnderCursor`; the editor's tail is a `DROP TABLE` a reload must not auto-run.
    reset();
    sandbox.isMultiQuery = true;
    sandbox.query_area.value = 'SELECT 1; DROP TABLE important';
    active().query = 'SELECT 1; DROP TABLE important';
    sandbox.query_area.selectionStart = 3;   /// caret inside `SELECT 1`, no selection
    sandbox.query_area.selectionEnd = 3;
    sandbox.getQueryUnderCursor = sandbox.realGetQueryUnderCursor;
    sandbox.tokenize = async () => [
        { token: 'SELECT', significant: true }, { token: ' ', significant: false },
        { token: '1', significant: true }, { token: ';', significant: true },
        { token: ' ', significant: false }, { token: 'DROP', significant: true },
        { token: ' ', significant: false }, { token: 'TABLE', significant: true },
        { token: ' ', significant: false }, { token: 'important', significant: true },
    ];
    const oneSubsetPromise = sandbox.postOne();
    await drain();
    resolvePendingPostImpl();
    await oneSubsetPromise;
    await drain();
    assert_eq('run-one partial: the entry is not stamped run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);
    assert_eq('run-one partial: the result snapshot is the statement that ran', active().result && active().result.query, 'SELECT 1;');
    sandbox.getQueryUnderCursor = async () => '';
    sandbox.isMultiQuery = false;
    await reload();
    assert_eq('run-one partial: a reload does not auto-run the DROP', sandbox.postAllCalled, false);

    /// Control: a full "Run all" of several statements (REAL `postAll` -> real `postMulti`,
    /// `partial` false) stays auto-runnable — reloading its `run=1` URL re-runs the whole editor,
    /// the behavior the partial cases above deliberately drop.
    reset();
    sandbox.query_area.value = 'SELECT 1; SELECT 2';
    active().query = 'SELECT 1; SELECT 2';
    sandbox.splitAllQueries = async text => text.split(';').map((q, i) => ({ query: q.trim(), is_select: true, start: i * 10, end: i * 10 + 8, queryStart: i * 10 }));
    const allFullPromise = sandbox.realPostAll();
    await drain();
    resolvePendingPostImpl();
    await allFullPromise;
    await drain();
    assert_eq('run-all full: the entry keeps run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), true);
    await reload();
    assert_eq('run-all full: a reload re-runs the whole editor', sandbox.postAllCalled, true);

    /// A full "Run all" that ERRORS partway is also partial: `postMulti` runs the statements group
    /// by group and breaks on the first failing group, so every statement after it is never launched
    /// (its slot in `collected` stays `null`). Recording such a run as `run=1` — as a full "Run all"
    /// otherwise is — would let a reload / shared link `postAll` the whole editor and execute a
    /// statement that never ran here (a destructive `DROP TABLE`). The entry must drop `run=1` just
    /// like a "Run selected" subset, and a reload must not auto-run. Drives the REAL `postMulti` with
    /// a middle statement that errors and a `DROP` after it (the `DROP` is skipped, never launched).
    reset();
    sandbox.query_area.value = 'SELECT 1; BAD; DROP TABLE important';
    active().query = 'SELECT 1; BAD; DROP TABLE important';
    const savedPostImpl = sandbox.postImpl;
    /// Resolve immediately: `SELECT 1` succeeds, the middle `BAD` statement errors (so the run stops
    /// and the trailing `DROP TABLE` is never launched — it would succeed if it were ever reached).
    sandbox.postImpl = (posted_request_num, query) => Promise.resolve({
        cancelled: false, is_error: query === 'BAD', response_ok: query !== 'BAD',
        format: 'JSONCompact', reply: 'result of ' + query,
        is_table: false, is_raw: false, is_chart: false, is_image: false,
    });
    await sandbox.postMulti(sandbox.request_num, [
        { query: 'SELECT 1', is_select: true },
        { query: 'BAD', is_select: false },
        { query: 'DROP TABLE important', is_select: false },
    ], false, 'SELECT 1; BAD; DROP TABLE important');
    await drain();
    sandbox.postImpl = savedPostImpl;
    assert_eq('run-all error: a run that skipped a statement drops run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);
    await reload();
    assert_eq('run-all error: a reload does not auto-run the skipped DROP', sandbox.postAllCalled, false);

    /// Control: "Run one" in multi-query mode on a SINGLE-statement editor is a full run — running
    /// it IS reproducible by `postAll` on reload — so `run=1` must be KEPT. Pins that `partial` is
    /// derived from whether the editor holds more than one statement (`queryUnderCursorIsPartial`),
    /// not merely from being in multi-query mode. Real `getQueryUnderCursor`, one-statement editor.
    reset();
    sandbox.isMultiQuery = true;
    sandbox.query_area.value = 'SELECT 1';
    active().query = 'SELECT 1';
    sandbox.query_area.selectionStart = 3;
    sandbox.query_area.selectionEnd = 3;
    sandbox.getQueryBoundaries = () => [{}];   /// one statement ⇒ should_select_query false ⇒ not partial
    sandbox.getQueryUnderCursor = sandbox.realGetQueryUnderCursor;
    sandbox.tokenize = async () => [
        { token: 'SELECT', significant: true }, { token: ' ', significant: false }, { token: '1', significant: true },
    ];
    const oneFullPromise = sandbox.postOne();
    await drain();
    resolvePendingPostImpl();
    await oneFullPromise;
    await drain();
    assert_eq('run-one single-statement: the full run keeps run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), true);
    sandbox.getQueryBoundaries = () => [{}, {}];   /// restore the default stub
    sandbox.getQueryUnderCursor = async () => '';
    sandbox.isMultiQuery = false;

    /// A query edit that drops a parameter placeholder rebuilds the `param_*` inputs (via
    /// `updateQueryParams`) but leaves `tab.params` holding the previous query's bindings until a
    /// later capture. When the edited query is then run, `saveHistory` must compare against the LIVE
    /// parameter inputs, not the stale `tab.params`: otherwise the removed placeholder is mistaken for
    /// an in-flight parameter edit, which drops `run=1` and leaks `param_x` into the entry and the URL.
    reset();
    await run('SELECT 0');
    type('SELECT {x:Int32}');
    setParam('x', '1');
    await run('SELECT {x:Int32}');
    /// The edited query no longer references {x}, so its `param_x` input is gone and the live
    /// snapshot drops it -- but `tab.params` still lists it. Run the edited query immediately.
    delete sandbox.param_inputs.x;
    await run('SELECT 1');
    assert_eq('placeholder dropped: the completed run keeps run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), true);
    assert_eq('placeholder dropped: the removed placeholder does not leak into the URL', sandbox.history.stack[sandbox.history.idx].url.includes('param_x'), false);
    assert_params('placeholder dropped: the stale binding is cleared from the tab', active().params, {});

    /// A run launched while a tab activation / Back-Forward parameter restore is still pending must
    /// record the destination tab's own params and keep `run=1`, even though the live `param_*`
    /// inputs still show the previous tab. Switch from `SELECT {x}` (x=1) to `SELECT {y}` (y=2) and
    /// press Run before the restore finishes: `resolveRunParams` correctly runs `{y:'2'}`, but the
    /// aborted restore's `updateQueryParams` can rebuild the inputs blank (`{y:''}`) from the
    /// previous tab's `oldValues`. If `saveHistory` reread those inputs it would falsely see the
    /// params diverge, drop `run=1`, and persist `{y:''}` on a clean run. Drives the REAL `postOne`/
    /// `postSingle`/`resolveRunParams`, with the pending restore modeled by
    /// `params_restore_pending_token === request_num` and stale/blank live inputs.
    reset();
    sandbox.param_inputs = { x: '1' };
    active().query = 'SELECT {x}';
    sandbox.query_area.value = 'SELECT {x}';
    await run('SELECT {x}');                        /// tab A: a clean, run-backed entry with x=1
    /// A second tab (SELECT {y}, y=2) is being activated; its parameter restore is still in flight
    /// (`params_restore_pending_token === request_num`) when the user presses Run. `restoreFromHistory`
    /// has set the editor to the destination query but not yet written its params, so the live inputs
    /// still hold tab A's `{x:'1'}`.
    const dest_tab = sandbox.makeTab();
    dest_tab.query = 'SELECT {y}';
    dest_tab.params = { y: '2' };
    sandbox.tabs.push(dest_tab);
    sandbox.activeTabId = dest_tab.id;
    sandbox.query_area.value = 'SELECT {y}';
    sandbox.param_inputs = { x: '1' };
    sandbox.params_restore_pending_token = sandbox.request_num;
    sandbox.params_restore_pending_query = 'SELECT {y}';   /// the query the restore was for; unchanged here
    sandbox.isMultiQuery = false;
    sandbox.query_area.selectionStart = 0;
    sandbox.query_area.selectionEnd = 0;
    let releaseCursorDest;
    sandbox.getQueryUnderCursor = () => new Promise(resolve => { releaseCursorDest = () => resolve('SELECT {y}'); });
    const destRunPromise = sandbox.postOne();
    await drain();
    releaseCursorDest();
    await drain();
    /// The aborted restore's `updateQueryParams` rebuilds the inputs from tab A's `oldValues`, so
    /// `param-y` ends up blank (and `param-x` is gone) by the time the run completes.
    sandbox.param_inputs = { y: '' };
    resolvePendingPostImpl();
    await destRunPromise;
    await drain();
    assert_eq('run under pending param restore: the clean run keeps run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), true);
    assert_params('run under pending param restore: the entry keeps the destination tab params', active().params, { y: '2' });
    assert_eq('run under pending param restore: the destination param reaches the URL', sandbox.history.stack[sandbox.history.idx].url.includes('param_y=2'), true);
    assert_eq('run under pending param restore: the stale source param does not leak', sandbox.history.stack[sandbox.history.idx].url.includes('param_x'), false);
    /// A later `captureActiveTab` (the debounced persist, a tab switch/rename, ...) rereads the live
    /// inputs; because the run wrote the destination params back into them, that capture keeps the
    /// right values instead of clobbering the tab with the stale/blank DOM.
    sandbox.captureActiveTab();
    assert_params('run under pending param restore: a later capture keeps the destination params', active().params, { y: '2' });
    sandbox.params_restore_pending_token = -1;
    sandbox.params_restore_pending_query = null;
    sandbox.getQueryUnderCursor = async () => '';

    /// A run launched under a pending param restore, but AFTER the user edited the destination
    /// query, must NOT stamp the destination tab's saved params back: they belong to the OLD query.
    /// Switch toward `SELECT {y}` (saved y=2), then edit the editor to a placeholder-free `SELECT 1`
    /// and press Run before the restore finishes. `resolveRunParams` must see the launched query no
    /// longer matches the restore's destination (`params_restore_pending_query`) and fall back to the
    /// live inputs (which the edit's `updateQueryParams` cleared for the new query), so the clean run
    /// records no `param_y`/`param_x` and stays runnable — not `/play?run=1&param_y=2#SELECT 1`.
    reset();
    sandbox.param_inputs = { x: '1' };
    active().query = 'SELECT {x}';
    sandbox.query_area.value = 'SELECT {x}';
    await run('SELECT {x}');                        /// tab A: a clean, run-backed entry with x=1
    const dest_tab_edited = sandbox.makeTab();
    dest_tab_edited.query = 'SELECT {y}';
    dest_tab_edited.params = { y: '2' };
    sandbox.tabs.push(dest_tab_edited);
    sandbox.activeTabId = dest_tab_edited.id;
    /// The restore set the editor to the destination query and armed the pending markers, but the
    /// user then edited the query to `SELECT 1` (its `updateQueryParams` rebuilt the inputs empty).
    sandbox.params_restore_pending_token = sandbox.request_num;
    sandbox.params_restore_pending_query = 'SELECT {y}';
    sandbox.query_area.value = 'SELECT 1';
    active().query = 'SELECT 1';
    sandbox.param_inputs = {};
    sandbox.isMultiQuery = false;
    sandbox.query_area.selectionStart = 0;
    sandbox.query_area.selectionEnd = 0;
    let releaseCursorEdited;
    sandbox.getQueryUnderCursor = () => new Promise(resolve => { releaseCursorEdited = () => resolve('SELECT 1'); });
    const editedRunPromise = sandbox.postOne();
    await drain();
    releaseCursorEdited();
    await drain();
    resolvePendingPostImpl();
    await editedRunPromise;
    await drain();
    assert_eq('edited query under pending restore: the clean run keeps run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), true);
    assert_eq('edited query under pending restore: the stale destination param does not leak', sandbox.history.stack[sandbox.history.idx].url.includes('param_y'), false);
    assert_eq('edited query under pending restore: the source param does not leak either', sandbox.history.stack[sandbox.history.idx].url.includes('param_x'), false);
    assert_params('edited query under pending restore: the entry carries no stale params', active().params, {});
    sandbox.params_restore_pending_token = -1;
    sandbox.params_restore_pending_query = null;
    sandbox.getQueryUnderCursor = async () => '';

    /// The SAME edited-query-under-pending-restore race, but reproduced BEFORE the edit's own
    /// `updateQueryParams` has rebuilt the inputs. The live `param_*` inputs (and `currentQueryParams`,
    /// which `getParamValues` reads through) still describe the PREVIOUS tab, holding `{x:'1'}`. A
    /// fallback that read the live inputs here would stamp `param_x=1` onto the edited placeholder-free
    /// `SELECT 1` -- the very leak this contract fixes, just sourced from the other side of the async
    /// rebuild (the earlier case set `param_inputs = {}`, the already-rebuilt path, so it could not
    /// catch this). The fix derives the launched query's own parameters from its TEXT
    /// (`resolveRunParams` -> `paramValuesForQuery` -> real `tokenize`/`extractQueryParams`), so
    /// `SELECT 1` records no parameters regardless of what the not-yet-rebuilt inputs still show.
    reset();
    sandbox.param_inputs = { x: '1' };
    active().query = 'SELECT {x}';
    sandbox.query_area.value = 'SELECT {x}';
    await run('SELECT {x}');                        /// tab A: a clean, run-backed entry with x=1
    const dest_tab_race = sandbox.makeTab();
    dest_tab_race.query = 'SELECT {y}';
    dest_tab_race.params = { y: '2' };
    sandbox.tabs.push(dest_tab_race);
    sandbox.activeTabId = dest_tab_race.id;
    sandbox.params_restore_pending_token = sandbox.request_num;
    sandbox.params_restore_pending_query = 'SELECT {y}';
    /// The user edited the editor to `SELECT 1`, but the edit's async `updateQueryParams` has NOT run
    /// yet, so the live inputs (and `currentQueryParams`) still hold the previous tab's `{x:'1'}`.
    sandbox.query_area.value = 'SELECT 1';
    active().query = 'SELECT 1';
    sandbox.param_inputs = { x: '1' };
    sandbox.currentQueryParams = [{ name: 'x', type: 'String' }];
    /// The real `paramValuesForQuery` tokenizes the LAUNCH query to find its own placeholders;
    /// `SELECT 1` has none, so the derived parameter set is empty regardless of the stale inputs.
    sandbox.tokenize = async () => [
        { token: 'SELECT', significant: true }, { token: ' ', significant: false }, { token: '1', significant: true },
    ];
    sandbox.isMultiQuery = false;
    sandbox.query_area.selectionStart = 0;
    sandbox.query_area.selectionEnd = 0;
    let releaseCursorRace;
    sandbox.getQueryUnderCursor = () => new Promise(resolve => { releaseCursorRace = () => resolve('SELECT 1'); });
    const raceRunPromise = sandbox.postOne();
    await drain();
    releaseCursorRace();
    await drain();
    resolvePendingPostImpl();
    await raceRunPromise;
    await drain();
    assert_eq('edited query under pending restore, inputs not yet rebuilt: the clean run keeps run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), true);
    assert_eq('edited query under pending restore, inputs not yet rebuilt: the stale source param does not leak', sandbox.history.stack[sandbox.history.idx].url.includes('param_x'), false);
    assert_eq('edited query under pending restore, inputs not yet rebuilt: no destination param leaks either', sandbox.history.stack[sandbox.history.idx].url.includes('param_y'), false);
    assert_params('edited query under pending restore, inputs not yet rebuilt: the entry carries no stale params', active().params, {});
    sandbox.params_restore_pending_token = -1;
    sandbox.params_restore_pending_query = null;
    sandbox.getQueryUnderCursor = async () => '';
    sandbox.currentQueryParams = [];

    /// A run launched under a pending param restore, then a NEWER trusted PARAMETER edit while the
    /// request is still in flight, must NOT stamp the launch-time snapshot back over the live edit:
    /// typing / editing does not cancel a request, so the destination tab's saved params are
    /// authoritative only until the user moves on. Switch toward `SELECT {y}` (saved y=2), press Run
    /// before the restore finishes, then change y to 9 while the run is in flight. `saveHistory` must
    /// leave the live `{y:'9'}` alone, drop `run=1` (the shown binding was never run), and record
    /// `{y:'9'}` — never restore `{y:'2'}` over it (which, comparing the snapshot against itself,
    /// would also keep `run=1`, so a reload / shared link would replay the stale binding). Drives the
    /// REAL `postOne`/`postSingle`/`resolveRunParams`.
    reset();
    sandbox.param_inputs = { x: '1' };
    active().query = 'SELECT {x}';
    sandbox.query_area.value = 'SELECT {x}';
    await run('SELECT {x}');                        /// tab A: a clean, run-backed entry with x=1
    const dest_tab_param_edit = sandbox.makeTab();
    dest_tab_param_edit.query = 'SELECT {y}';
    dest_tab_param_edit.params = { y: '2' };
    sandbox.tabs.push(dest_tab_param_edit);
    sandbox.activeTabId = dest_tab_param_edit.id;
    sandbox.query_area.value = 'SELECT {y}';
    sandbox.param_inputs = {};                      /// the restore has not written the destination inputs yet
    sandbox.params_restore_pending_token = sandbox.request_num;
    sandbox.params_restore_pending_query = 'SELECT {y}';   /// the query the restore was for; unchanged here
    sandbox.isMultiQuery = false;
    sandbox.query_area.selectionStart = 0;
    sandbox.query_area.selectionEnd = 0;
    let releaseCursorParamEdit;
    sandbox.getQueryUnderCursor = () => new Promise(resolve => { releaseCursorParamEdit = () => resolve('SELECT {y}'); });
    const paramEditRunPromise = sandbox.postOne();
    await drain();
    releaseCursorParamEdit();
    await drain();
    /// The user edits the destination parameter (y: 2 -> 9) while the run is still in flight; the
    /// real `param-` input handler persists it into `tab.params` and stamps the entry (`syncHistory`).
    setParam('y', '9');
    resolvePendingPostImpl();
    await paramEditRunPromise;
    await drain();
    assert_eq('param edit under pending restore mid-flight: the run drops run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);
    assert_params('param edit under pending restore mid-flight: the live param edit survives', active().params, { y: '9' });
    assert_eq('param edit under pending restore mid-flight: the live param input keeps the edit', sandbox.param_inputs.y, '9');
    assert_eq('param edit under pending restore mid-flight: the stale launch binding does not reach the URL', sandbox.history.stack[sandbox.history.idx].url.includes('param_y=2'), false);
    sandbox.params_restore_pending_token = -1;
    sandbox.params_restore_pending_query = null;
    sandbox.getQueryUnderCursor = async () => '';

    /// The same hazard, but the newer trusted edit is to the QUERY (not a parameter) while the run is
    /// in flight. Switch toward `SELECT {y}` (saved y=2), press Run before the restore finishes, then
    /// edit the editor to `SELECT {z}` (its `updateQueryParams` rebuilds the inputs for the new query).
    /// The launch-time snapshot's params belong to the OLD query, so `saveHistory` must NOT restore
    /// `{y:'2'}` under the new draft — that would produce an incoherent `#SELECT {z}` entry carrying
    /// `param_y=2`. It must leave the live draft alone, record the coherent `{z:''}`, and drop `run=1`.
    reset();
    sandbox.param_inputs = { x: '1' };
    active().query = 'SELECT {x}';
    sandbox.query_area.value = 'SELECT {x}';
    await run('SELECT {x}');                        /// tab A: a clean, run-backed entry with x=1
    const dest_tab_query_edit = sandbox.makeTab();
    dest_tab_query_edit.query = 'SELECT {y}';
    dest_tab_query_edit.params = { y: '2' };
    sandbox.tabs.push(dest_tab_query_edit);
    sandbox.activeTabId = dest_tab_query_edit.id;
    sandbox.query_area.value = 'SELECT {y}';
    sandbox.param_inputs = {};
    sandbox.params_restore_pending_token = sandbox.request_num;
    sandbox.params_restore_pending_query = 'SELECT {y}';
    sandbox.isMultiQuery = false;
    sandbox.query_area.selectionStart = 0;
    sandbox.query_area.selectionEnd = 0;
    let releaseCursorQueryEdit;
    sandbox.getQueryUnderCursor = () => new Promise(resolve => { releaseCursorQueryEdit = () => resolve('SELECT {y}'); });
    const queryEditRunPromise = sandbox.postOne();
    await drain();
    releaseCursorQueryEdit();
    await drain();
    /// The user edits the query to `SELECT {z}` while the run is in flight; the editor's `input`
    /// handler keeps `tab.query` in sync and `updateQueryParams` rebuilds the inputs for the new query.
    type('SELECT {z}');
    sandbox.param_inputs = { z: '' };
    /// `saveHistory`'s diverged-query branch derives the draft's params from its TEXT (`SELECT {z}`),
    /// so tokenize it: `param-z` (present, blank after the rebuild) is read and the coherent `{z:''}`
    /// recorded, never the launch snapshot's stale `{y:'2'}`.
    sandbox.tokenize = async () => [
        { token: 'SELECT', significant: true }, { token: ' ', significant: false },
        { token: '{', significant: true }, { token: 'z', significant: true },
        { token: ':', significant: true }, { token: 'String', significant: true },
        { token: '}', significant: true },
    ];
    resolvePendingPostImpl();
    await queryEditRunPromise;
    await drain();
    assert_eq('query edit under pending restore mid-flight: the run drops run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);
    assert_eq('query edit under pending restore mid-flight: the live draft query survives', active().query, 'SELECT {z}');
    assert_params('query edit under pending restore mid-flight: the entry carries the new query params, not the old', active().params, { z: '' });
    assert_eq('query edit under pending restore mid-flight: the stale destination param does not leak into the URL', sandbox.history.stack[sandbox.history.idx].url.includes('param_y'), false);
    sandbox.tokenize = async () => [];
    sandbox.params_restore_pending_token = -1;
    sandbox.params_restore_pending_query = null;
    sandbox.getQueryUnderCursor = async () => '';

    /// The trusted param edit lands even EARLIER: while `resolveRunParams` is still awaiting the
    /// edited launch query's own `paramValuesForQuery` tokenization, before any request is issued.
    /// The pending-restore guard's launch snapshot (`live_params_at_launch`) is captured
    /// synchronously at click time in `postOne`/`postAll` — capturing it inside `postSingle`/
    /// `postMulti` AFTER the `resolveRunParams` await would fold this edit into the "launch" state,
    /// so `saveHistory` could not see the divergence: it would write the click-time resolved params
    /// back over the newer edit and keep `run=1` for a binding the user no longer sees (a reload /
    /// shared link would replay it). Start from a tab showing `SELECT {y}` with y=1 in the inputs,
    /// switch toward another `SELECT {y}` tab (saved y=2) whose restore is still pending, edit the
    /// query (so `resolveRunParams` takes the tokenizing `paramValuesForQuery` path), press Run,
    /// change y to 9 while that tokenization is pending, then let the run finish. The request
    /// executes with the click-time `{y:'1'}`; the live `{y:'9'}` must survive and the entry must
    /// drop `run=1`. Drives the REAL `postOne`/`postSingle`/`resolveRunParams`/`paramValuesForQuery`
    /// with a hung `tokenize`.
    reset();
    sandbox.param_inputs = { y: '1' };
    sandbox.currentQueryParams = [{ name: 'y', type: 'String' }];
    active().query = 'SELECT {y}';
    sandbox.query_area.value = 'SELECT {y}';
    await run('SELECT {y}');                        /// tab A: a clean, run-backed entry with y=1
    const dest_tab_early_edit = sandbox.makeTab();
    dest_tab_early_edit.query = 'SELECT {y}';
    dest_tab_early_edit.params = { y: '2' };
    sandbox.tabs.push(dest_tab_early_edit);
    sandbox.activeTabId = dest_tab_early_edit.id;
    sandbox.params_restore_pending_token = sandbox.request_num;
    sandbox.params_restore_pending_query = 'SELECT {y}';
    /// The user edits the editor BEFORE pressing Run, so the launch query diverges from the
    /// restore's destination and `resolveRunParams` derives the params from the query text
    /// (`paramValuesForQuery`), awaiting the hung tokenization below. The previous tab's live
    /// input (`{y:'1'}`) is the click-time snapshot the run executes with.
    type('SELECT {y} -- edited');
    sandbox.isMultiQuery = false;
    sandbox.query_area.selectionStart = 0;
    sandbox.query_area.selectionEnd = 0;
    sandbox.getQueryUnderCursor = async () => 'SELECT {y} -- edited';
    let releaseEarlyEditTokenize;
    sandbox.tokenize = () => new Promise(resolve => { releaseEarlyEditTokenize = () => resolve([
        { token: 'SELECT', significant: true }, { token: ' ', significant: false },
        { token: '{', significant: true }, { token: 'y', significant: true },
        { token: ':', significant: true }, { token: 'String', significant: true },
        { token: '}', significant: true }, { token: ' ', significant: false },
        { token: '-- edited', significant: false },
    ]); });
    const earlyEditRunPromise = sandbox.postOne();
    await drain();
    setParam('y', '9');                             /// a trusted edit during the resolution await
    releaseEarlyEditTokenize();                     /// the launch's tokenization finally resolves
    await drain();
    resolvePendingPostImpl();
    await earlyEditRunPromise;
    await drain();
    assert_params('param edit during launch param resolution: the run executes with the click-time binding', active().result && active().result.params, { y: '1' });
    assert_params('param edit during launch param resolution: the live param edit survives', active().params, { y: '9' });
    assert_eq('param edit during launch param resolution: the live param input keeps the edit', sandbox.param_inputs.y, '9');
    assert_eq('param edit during launch param resolution: the run drops run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);
    assert_eq('param edit during launch param resolution: the stale click-time binding does not reach the URL', sandbox.history.stack[sandbox.history.idx].url.includes('param_y=1'), false);
    sandbox.tokenize = async () => [];
    sandbox.params_restore_pending_token = -1;
    sandbox.params_restore_pending_query = null;
    sandbox.getQueryUnderCursor = async () => '';
    sandbox.currentQueryParams = [];

    /// The SAME stale-placeholder race, but on the ORDINARY run path -- NO tab restore in flight.
    /// Editing the query only STARTS `updateQueryParams`; it does not replace `currentQueryParams` /
    /// the `param_*` inputs until after its own `await tokenize(...)`. A `Run` pressed before that
    /// rebuild lands (cold page / slow first lexer load) must still execute the edited query with ITS
    /// OWN placeholders, never the previous query's stale bindings. Change `SELECT {x}` (x=1) to
    /// `SELECT {y}` and Run before the inputs are rebuilt: `resolveRunParams` -> `paramValuesForQuery`
    /// derives `{y}` from the launched query TEXT (reading `param-y` if that input exists, else blank),
    /// so the run records the destination `param_y` and never the stale `param_x=1`. The pre-fix
    /// ordinary path read the live inputs (`getParamValues`) here, which would leak `param_x=1` onto
    /// `SELECT {y}` -- the very race the pending-restore path already fixes, just without a tab switch.
    /// Drives the REAL `postOne`/`postSingle`/`resolveRunParams`; the edit's own `updateQueryParams`
    /// rebuild lands during the (hung) network round-trip, so the clean run still keeps run=1.
    reset();
    sandbox.param_inputs = { x: '1' };
    active().query = 'SELECT {x}';
    sandbox.query_area.value = 'SELECT {x}';
    sandbox.currentQueryParams = [{ name: 'x', type: 'String' }];
    await run('SELECT {x}');                        /// a clean, run-backed entry with x=1
    /// No restore is pending for this run -- the ordinary path (`params_restore_pending` false).
    sandbox.params_restore_pending_token = -1;
    sandbox.params_restore_pending_query = null;
    /// The user edited the query to `SELECT {y}`, but the edit's async `updateQueryParams` has NOT
    /// rebuilt the inputs yet, so the live inputs (and `currentQueryParams`) still hold `{x:'1'}`.
    sandbox.query_area.value = 'SELECT {y}';
    active().query = 'SELECT {y}';
    sandbox.param_inputs = { x: '1' };
    sandbox.currentQueryParams = [{ name: 'x', type: 'String' }];
    /// `paramValuesForQuery` tokenizes the LAUNCH query (`SELECT {y}`) to enumerate its own placeholder;
    /// `param-y` does not exist yet, so its value derives as blank -- never the stale `param-x`.
    sandbox.tokenize = async () => [
        { token: 'SELECT', significant: true }, { token: ' ', significant: false },
        { token: '{', significant: true }, { token: 'y', significant: true },
        { token: ':', significant: true }, { token: 'String', significant: true },
        { token: '}', significant: true },
    ];
    sandbox.isMultiQuery = false;
    sandbox.query_area.selectionStart = 0;
    sandbox.query_area.selectionEnd = 0;
    let releaseCursorOrdinary;
    sandbox.getQueryUnderCursor = () => new Promise(resolve => { releaseCursorOrdinary = () => resolve('SELECT {y}'); });
    const ordinaryRacePromise = sandbox.postOne();
    await drain();
    releaseCursorOrdinary();
    await drain();
    /// The edit's own `updateQueryParams` rebuild lands while the request is still in flight, so by
    /// the time the run completes the inputs describe `SELECT {y}` with a (blank) `param-y`.
    sandbox.param_inputs = { y: '' };
    sandbox.currentQueryParams = [{ name: 'y', type: 'String' }];
    resolvePendingPostImpl();
    await ordinaryRacePromise;
    await drain();
    assert_eq('ordinary run, inputs not yet rebuilt: the clean run keeps run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), true);
    assert_eq('ordinary run, inputs not yet rebuilt: the stale source param does not leak', sandbox.history.stack[sandbox.history.idx].url.includes('param_x'), false);
    assert_eq('ordinary run, inputs not yet rebuilt: the launched query param reaches the URL', sandbox.history.stack[sandbox.history.idx].url.includes('param_y='), true);
    assert_params('ordinary run, inputs not yet rebuilt: the run snapshot carries the launched query params', active().result && active().result.params, { y: '' });
    sandbox.getQueryUnderCursor = async () => '';
    sandbox.tokenize = async () => [];

    /// The same stale-input race on the SAVE side (`saveHistory`'s diverged-query branch): a query
    /// edited WHILE a run is in flight, whose async `updateQueryParams` rebuild has NOT landed by the
    /// time the run completes. A run of `SELECT {x:Int32}` (x=1) is launched, then the editor is edited
    /// to a DIFFERENT placeholder set `SELECT {y:Int32}`, and the run completes before the rebuild
    /// lands — so the live inputs still describe `{x}`. `saveHistory` correctly drops `run=1` (the query
    /// diverged), but must record the DRAFT's own params, derived from its query TEXT
    /// (`paramValuesForQuery`), NOT `getParamValues()` — the latter here still reads the stale
    /// `param_x=1` and would leak it into the `SELECT {y:Int32}` entry / URL under a query that no
    /// longer has an `x`. This combines the `delayed completion` (diverged query) and `placeholder
    /// dropped` (rebuild not landed) hazards, which the harness otherwise only covers separately.
    reset();
    await run('SELECT 0');
    sandbox.param_inputs = { x: '1' };                  /// the run's live parameter binding
    const inFlightDivergedParams = startRun('SELECT {x:Int32}');   /// launch snapshot: query {x}, params {x:'1'}
    /// The user edits the query to a different placeholder set mid-flight; the edit's async
    /// `updateQueryParams` has NOT rebuilt the inputs yet, so they still hold the previous `{x:'1'}`.
    type('SELECT {y:Int32}');
    sandbox.param_inputs = { x: '1' };
    /// `paramValuesForQuery` tokenizes the DRAFT (`SELECT {y:Int32}`) to enumerate its own placeholder;
    /// `param-y` does not exist yet (rebuild not landed), so its value derives blank — never `param-x`.
    sandbox.tokenize = async () => [
        { token: 'SELECT', significant: true }, { token: ' ', significant: false },
        { token: '{', significant: true }, { token: 'y', significant: true },
        { token: ':', significant: true }, { token: 'Int32', significant: true },
        { token: '}', significant: true },
    ];
    await finishRun(inFlightDivergedParams);
    sandbox.tokenize = async () => [];
    assert_eq('diverged query mid-flight, inputs not yet rebuilt: the live draft survives', active().query, 'SELECT {y:Int32}');
    assert_eq('diverged query mid-flight, inputs not yet rebuilt: the entry drops run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);
    assert_eq('diverged query mid-flight, inputs not yet rebuilt: the stale source param does not leak', sandbox.history.stack[sandbox.history.idx].url.includes('param_x'), false);
    assert_params("diverged query mid-flight, inputs not yet rebuilt: the entry carries the draft's own params, not the stale ones", active().params, { y: '' });
    assert_eq("diverged query mid-flight, inputs not yet rebuilt: the completed run's result is still kept", active().result && active().result.query, 'SELECT {x:Int32}');

    /// A run whose parameters are the SAME bindings as the live inputs but enumerated in a different
    /// KEY ORDER. Reordering `SELECT {x}, {y}` to `SELECT {y}, {x}` and pressing Run before the edit's
    /// `updateQueryParams` rebuild lands makes the run's `stateData.params` follow the launched query
    /// TEXT (`paramValuesForQuery` -> `{y, x}`), while the live inputs `getParamValues()` still iterate
    /// the previous placeholder order (`{x, y}`). The two describe the same bindings, so the entry must
    /// keep `run=1` and a later tab switch must not re-mark the tab dirty. A serialized insertion-order
    /// comparison (`JSON.stringify`) would see `{x, y}` and `{y, x}` as different, wrongly drop `run=1`
    /// (in `saveHistory`), and keep `tabReflectsRun` false (re-dropping it on the next switch). The
    /// order-insensitive `sameParams` comparison fixes both sites.
    reset();
    sandbox.param_inputs = { x: '1', y: '2' };   /// getParamValues -> insertion order {x, y}
    active().query = 'SELECT {y:Int32}, {x:Int32}';
    sandbox.query_area.value = 'SELECT {y:Int32}, {x:Int32}';
    await sandbox.saveHistory({
        query: 'SELECT {y:Int32}, {x:Int32}',
        resultQuery: 'SELECT {y:Int32}, {x:Int32}',
        params: { y: '2', x: '1' },   /// launched-query-text order from paramValuesForQuery: {y, x}
        format: 'JSONCompact', ok: true, data: 'result', elapsed_ns: 1,
        database: sandbox.selected_database, url: sandbox.url_elem.value, user: sandbox.user_elem.value });
    await drain();
    assert_eq('reordered params: the clean run keeps run=1 despite differing key order', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), true);
    sandbox.syncHistory();   /// a later tab switch calls this; the reordered bindings must not drop run=1
    assert_eq('reordered params: a later tab switch keeps run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), true);

    /// `resolveRunParams` is async (it tokenizes the launched query to enumerate its placeholders), so
    /// a Cancel or tab switch can supersede a run while it is still awaiting that tokenization on a
    /// cold page. `postSingle`/`postMulti` must bail immediately after that await — before `clear`,
    /// creating an `AbortController`, or issuing any request — otherwise the stale run clobbers the
    /// now-active tab's UI and sends a canceled query to the server (`cancel`/`invalidateInFlight` only
    /// bump `request_num`; there is no controller to abort yet at that point). Drives the REAL
    /// `postOne`/`postSingle` with a hung `tokenize`, supersedes the run mid-await (`invalidateInFlight`,
    /// what a tab switch / Cancel does), then releases: no `postImpl` call and no history entry written.
    reset();
    await run('SELECT 0');                        /// a clean, run-backed entry
    const stack_len_before_cancel = sandbox.history.stack.length;
    let postImplCalls = 0;
    const savedPostImplForCancel = sandbox.postImpl;
    sandbox.postImpl = (...a) => { postImplCalls++; return savedPostImplForCancel(...a); };
    active().query = 'SELECT {x:Int32}';
    sandbox.query_area.value = 'SELECT {x:Int32}';
    sandbox.isMultiQuery = false;
    sandbox.query_area.selectionStart = 0;
    sandbox.query_area.selectionEnd = 0;
    sandbox.getQueryUnderCursor = async () => 'SELECT {x:Int32}';
    let releaseCancelTokenize;
    /// Hang the run's `resolveRunParams` -> `paramValuesForQuery` -> `tokenize`, so the run suspends
    /// mid-await, exactly the cold-page window the finding describes.
    sandbox.tokenize = () => new Promise(resolve => { releaseCancelTokenize = () => resolve([
        { token: 'SELECT', significant: true }, { token: ' ', significant: false },
        { token: '{', significant: true }, { token: 'x', significant: true },
        { token: ':', significant: true }, { token: 'Int32', significant: true },
        { token: '}', significant: true },
    ]); });
    const cancelRunPromise = sandbox.postOne();
    await drain();
    sandbox.invalidateInFlight();                 /// a tab switch / Cancel supersedes the suspended run
    releaseCancelTokenize();                       /// the cold-page tokenize finally resolves
    await drain();
    /// The fixed path bailed before issuing any request, so this releases nothing; a regressed path
    /// that did not bail queued a hung `postImpl` here, so release it — otherwise the run would hang on
    /// it forever and the assertions below would never run (a silent stall instead of a loud failure).
    resolvePendingPostImpl();
    await cancelRunPromise;
    await drain();
    assert_eq('cancel during param resolution: the superseded run issues no request', postImplCalls, 0);
    assert_eq('cancel during param resolution: no history entry is written for the canceled run', sandbox.history.stack.length, stack_len_before_cancel);
    assert_eq("cancel during param resolution: the previous run's result snapshot is untouched", active().result && active().result.query, 'SELECT 0');
    sandbox.postImpl = savedPostImplForCancel;
    sandbox.tokenize = async () => [];
    sandbox.getQueryUnderCursor = async () => '';

    /// A parameter input moved WHILE the launch's own param resolution is still pending. The run
    /// entrypoints snapshot the live inputs synchronously in the click's task (`postOne`/`postAll`
    /// -> `live_params`), and `paramValuesForQuery` filters that snapshot — never a post-await
    /// re-read of the inputs. Press Run with x=1, change x to 2 while the cold-page tokenization
    /// is pending: the request and the result snapshot must carry the click-time `{x:'1'}` while
    /// the live input keeps the newer 2 — which then diverges from what ran, so `run=1` drops. A
    /// regression that read the inputs after the tokenization await would execute the click with
    /// `{x:'2'}`, a binding the user never launched. Drives the REAL `postOne`/`postSingle`/
    /// `resolveRunParams`/`paramValuesForQuery` with a hung `tokenize`.
    reset();
    sandbox.param_inputs = { x: '1' };
    sandbox.currentQueryParams = [{ name: 'x', type: 'String' }];
    active().query = 'SELECT {x:Int32}';
    active().params = { x: '1' };
    sandbox.query_area.value = 'SELECT {x:Int32}';
    sandbox.isMultiQuery = false;
    sandbox.query_area.selectionStart = 0;
    sandbox.query_area.selectionEnd = 0;
    sandbox.getQueryUnderCursor = async () => 'SELECT {x:Int32}';
    let releaseMovedInputTokenize;
    sandbox.tokenize = () => new Promise(resolve => { releaseMovedInputTokenize = () => resolve([
        { token: 'SELECT', significant: true }, { token: ' ', significant: false },
        { token: '{', significant: true }, { token: 'x', significant: true },
        { token: ':', significant: true }, { token: 'Int32', significant: true },
        { token: '}', significant: true },
    ]); });
    const movedInputRunPromise = sandbox.postOne();
    await drain();
    setParam('x', '2');                            /// the user moves the input mid-resolution
    releaseMovedInputTokenize();                   /// the cold-page tokenize finally resolves
    await drain();
    resolvePendingPostImpl();
    await movedInputRunPromise;
    await drain();
    assert_params('param moved during launch resolution: the run executes with the click-time binding', active().result && active().result.params, { x: '1' });
    assert_eq('param moved during launch resolution: the live input keeps the newer value', sandbox.param_inputs.x, '2');
    assert_params('param moved during launch resolution: the tab records the live binding', active().params, { x: '2' });
    assert_eq('param moved during launch resolution: the entry drops run=1 (shown binding was never run)', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);
    sandbox.tokenize = async () => [];
    sandbox.getQueryUnderCursor = async () => '';
    sandbox.currentQueryParams = [];

    /// A SECOND draft edit while `saveHistory`'s diverged-branch derivation is still awaiting the
    /// FIRST draft's tokenization. `writeHistoryEntry` serializes the THEN-current `tab.query`, so
    /// params derived from the first draft must never be recorded under the second draft's text
    /// (an incoherent `#SELECT {z:Int32}` entry carrying `{y:''}`): the branch re-derives until the
    /// draft is stable. Run `SELECT 1`, edit to `SELECT {y:Int32}` mid-flight, complete the run
    /// with the diverged-branch tokenization hung, type `SELECT {z:Int32}` while it is pending,
    /// then release both derivations.
    reset();
    await run('SELECT 0');
    const secondEditRun = startRun('SELECT 1');
    type('SELECT {y:Int32}');                      /// first mid-flight edit: a diverged draft
    sandbox.param_inputs = {};                     /// its `updateQueryParams` rebuild never landed
    const pendingDraftTokenize = [];
    const draftTokens = name => [
        { token: 'SELECT', significant: true }, { token: ' ', significant: false },
        { token: '{', significant: true }, { token: name, significant: true },
        { token: ':', significant: true }, { token: 'Int32', significant: true },
        { token: '}', significant: true },
    ];
    sandbox.tokenize = q => new Promise(resolve =>
        pendingDraftTokenize.push(() => resolve(draftTokens(q.includes('{z') ? 'z' : 'y'))));
    const secondEditFinishPromise = finishRun(secondEditRun);
    await drain();                                 /// `saveHistory` is now awaiting the first draft's tokenize
    type('SELECT {z:Int32}');                      /// the second edit lands while that await is pending
    pendingDraftTokenize.shift()();                /// first derivation resolves against the OLD draft
    await drain();                                 /// the branch sees the newer draft and re-derives
    /// A regressed single-shot derivation queues no second tokenize; guard the release so it fails
    /// on the assertions below (recorded `{y:''}` under `SELECT {z:Int32}`) rather than a TypeError.
    if (pendingDraftTokenize.length) { pendingDraftTokenize.shift()(); }
    await drain();
    await secondEditFinishPromise;
    await drain();
    assert_eq('second edit during delayed completion: the latest draft survives in the editor', active().query, 'SELECT {z:Int32}');
    assert_params("second edit during delayed completion: the entry carries the latest draft's own params", active().params, { z: '' });
    assert_eq('second edit during delayed completion: the first draft param does not leak into the URL', sandbox.history.stack[sandbox.history.idx].url.includes('param_y'), false);
    assert_eq('second edit during delayed completion: the entry drops run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);
    assert_eq("second edit during delayed completion: the completed run's result is still kept", active().result && active().result.query, 'SELECT 1');
    sandbox.tokenize = async () => [];

    /// A trusted PARAM edit while `saveHistory`'s diverged-branch derivation is still awaiting the
    /// draft's tokenization. `paramValuesForQuery` snapshots the live inputs when the derivation
    /// starts, so a binding edited during the await must trigger a re-derivation — the branch
    /// re-derives until the draft text AND its parameter values are stable — or the stale snapshot
    /// would be committed back into `tab.params` and the entry, silently reverting the newer value.
    /// Launch `SELECT {x:Int32}`, edit to `SELECT {y:Int32}` (its rebuild landed, y=2) mid-flight,
    /// complete the run with the diverged-branch tokenization hung, change y to 9 while it is
    /// pending, then release the derivations.
    reset();
    const paramEditRun = startRun('SELECT {x:Int32}');
    type('SELECT {y:Int32}');                      /// mid-flight edit: a diverged draft
    sandbox.param_inputs = { y: '2' };             /// the edit's `updateQueryParams` rebuild HAS landed
    sandbox.currentQueryParams = [{ name: 'y', type: 'String' }];
    active().params = { y: '2' };
    const pendingParamEditTokenize = [];
    sandbox.tokenize = () => new Promise(resolve =>
        pendingParamEditTokenize.push(() => resolve(draftTokens('y'))));
    const paramEditFinishPromise = finishRun(paramEditRun);
    await drain();                                 /// `saveHistory` is now awaiting the draft's tokenize
    setParam('y', '9');                            /// the user edits the binding while that await is pending
    pendingParamEditTokenize.shift()();            /// first derivation resolves against the OLD snapshot
    await drain();                                 /// the branch sees the newer binding and re-derives
    /// A regressed query-only stability check queues no second tokenize; guard the release so it
    /// fails on the assertions below (a reverted `{y:'2'}`) rather than a TypeError.
    if (pendingParamEditTokenize.length) { pendingParamEditTokenize.shift()(); }
    await drain();
    await paramEditFinishPromise;
    await drain();
    assert_params('param edit during draft derivation: the newer binding survives in the tab', active().params, { y: '9' });
    assert_eq('param edit during draft derivation: the live input keeps the newer value', sandbox.param_inputs.y, '9');
    assert_eq('param edit during draft derivation: the stale binding does not reach the URL', sandbox.history.stack[sandbox.history.idx].url.includes('param_y=2'), false);
    assert_eq('param edit during draft derivation: the entry drops run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), false);
    assert_eq("param edit during draft derivation: the completed run's result is still kept", active().result && active().result.query, 'SELECT {x:Int32}');
    sandbox.tokenize = async () => [];
    sandbox.currentQueryParams = [];

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

    /// After a stale reload preserved an unrun draft (refusing to auto-run the URL's stale
    /// `run=1`), a LATER genuine full run must still write a `run=1` entry: the suppression
    /// protects only the draft the user never ran, not queries they explicitly execute
    /// afterwards. A regression that suppressed `run=1` stamping for the whole session would
    /// write the rerun's entry without `run=1`, so reloading (or sharing) it would no longer
    /// re-run a query the user actually executed — unlike the clean-run reload control below.
    reset();
    await run('SELECT 1');
    type('SELECT 2');
    await reload();
    assert_eq('stale reload then rerun: the draft is restored unrun first', active().query, 'SELECT 2');
    assert_eq('stale reload then rerun: the draft is not auto-run', sandbox.postAllCalled, false);
    await run('SELECT 2');
    assert_eq('stale reload then rerun: the explicit rerun entry carries run=1 again', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), true);
    await reload();
    assert_eq('stale reload then rerun: reloading the rerun re-runs it', sandbox.postAllCalled, true);

    /// The stale-reload suppression must not leak into OTHER tabs either: after a reload
    /// preserved an unrun draft in one tab, switching to a different, still clean run-backed
    /// tab rewrites that tab's URL (`switchToTab` -> `syncHistory`), and the rewrite must keep
    /// `run=1` — the query it carries WAS fully executed, so reloading or sharing it must keep
    /// re-running it. A regression that suppressed stamping session-wide would drop the marker
    /// here; the draft's own refreshed entry must stay unstamped at the same time.
    reset();
    await run('SELECT 1');                     /// first tab: clean, run-backed
    const clean_tab_id = sandbox.activeTabId;
    sandbox.addTab();                          /// real addTab: folds the first tab's entry, activates the new one
    await drain();
    await run('SELECT 2');
    type('SELECT 3');                          /// unrun draft on top of the second tab's run
    await reload();                            /// stale reload: the draft is preserved, not auto-run
    assert_eq('draft in another tab: the draft is restored unrun', active().query, 'SELECT 3');
    assert_eq('draft in another tab: the draft is not auto-run', sandbox.postAllCalled, false);
    await sandbox.switchToTab(clean_tab_id);   /// real switch: refreshes the draft entry, syncs the clean tab
    await drain();
    assert_eq('draft in another tab: the clean tab is restored on switch', active().query, 'SELECT 1');
    assert_eq('draft in another tab: the clean tab rewrite keeps run=1', sandbox.history.stack[sandbox.history.idx].url.includes('run=1'), true);
    assert_eq('draft in another tab: the draft tab entry stays unstamped', sandbox.history.stack[sandbox.history.idx - 1].url.includes('run=1'), false);

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
