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
#     keep restoring verbatim).
# The harness extracts the real tab/history functions from the served /play page and
# drives them under node with stub DOM/history objects, asserting on the observable
# state: history entries, the active tab, and the editor.

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
    'saveHistory', 'syncHistory', 'resolveTabForState'];
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
sandbox.window = sandbox;
/// Browser-history stub: a stack of entries; Back/Forward fire `onpopstate` with the
/// state of the entry navigated to, exactly like a browser.
sandbox.history = {
    stack: [], idx: -1,
    get state() { return this.idx >= 0 ? this.stack[this.idx].state : null; },
    pushState(state, title, url) { this.stack.length = this.idx + 1; this.stack.push({ state, url }); this.idx++; },
    replaceState(state, title, url)
    {
        if (this.idx < 0) { this.stack.push({ state, url }); this.idx = 0; }
        else this.stack[this.idx] = { state, url };
    },
    back() { if (this.idx <= 0) throw new Error('nothing to go back to'); this.idx--; sandbox.window.onpopstate({ state: this.stack[this.idx].state }); },
    forward() { if (this.idx >= this.stack.length - 1) throw new Error('nothing to go forward to'); this.idx++; sandbox.window.onpopstate({ state: this.stack[this.idx].state }); },
};
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
    const tab = sandbox.makeTab();
    sandbox.tabs.push(tab);
    sandbox.activeTabId = tab.id;
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

    console.log('OK');
})().catch(e => { console.error('FAIL: ' + (e && e.stack || e)); process.exit(1); });
EOF

node "$harness" "$html"
rm -f "$html" "$harness"
