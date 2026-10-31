#!/usr/bin/env node
/// Executable regression harness for the `/play` EXPLAIN PLAN tree - the Plan view that renders an
/// `EXPLAIN` as a collapsible tree of plan nodes instead of the server's indented text.
///
/// Runs the REAL helpers extracted from the served `play.html` - the module-level functions between
/// `EXPLAIN_NON_PLAN_KIND_WORDS` and `SETTINGS_LIST_FOLLOWERS`, which are pure functions of their
/// arguments and need no DOM - plus `fallbackTokenize` and `TT` to build their token input. The
/// contracts pinned here:
///
///  - `explainPlanRequest` decides, from the tokens alone, whether a statement's plan can be drawn, and
///    where to splice `json = 1` in to make the Plan view's second call (the statement itself runs as
///    written). `PLAN` (or an
///    absent kind) is the only kind in scope, because it is the only one the server accepts `json`
///    for - every other kind is rejected with `UNKNOWN_SETTING`, so rewriting one would turn a
///    working query into an error;
///  - the insertion goes at the FRONT of the settings list, carrying the separating comma only when a
///    list is already there, so both `EXPLAIN PLAN json = 1, ... SELECT ...` and
///    `EXPLAIN PLAN json = 1, ..., indexes=1 SELECT ...` are valid SQL;
///  - `indexes`, `header` and `actions` are turned on alongside `json`, because they are what the
///    node details show (the output columns, the per-index pruning, a join's kind and clauses), and
///    they are purely additive - they attach sections to the nodes without changing the plan's shape.
///    An entry the statement already spells is never added again, so an explicit `indexes = 0` stands;
///  - a `json` setting the user wrote is never overwritten, in either direction: `json = 1` already
///    yields a tree, and `json = 0` is how the classic indented text is asked for back;
///  - the SVG path is untouched. A `digraph` response comes from `graph = 1`, a setting of
///    `EXPLAIN PIPELINE` and `EXPLAIN AST` only, so those queries are already out of scope by kind;
///  - the decision is made on TOKENS, so an `EXPLAIN` inside a string literal or a comment is not
///    mistaken for a statement, and the insertion offset survives leading comments, odd whitespace
///    and multi-byte characters (the tokens tile the text exactly, so the offset is the sum of the
///    token lengths before it - a sum that would be wrong if the insignificant tokens were dropped);
///  - `parseExplainPlan` normalizes the payload into the tree the view renders, splitting each node's
///    properties into the scalars shown inline and the array/object ones shown as collapsible
///    sub-nodes, naming the inputs of a join (`left`, `right · build`), and returning null for
///    anything that is not an `EXPLAIN PLAN json = 1` payload;
///  - the server accepts what the page sends: each statement runs, and the plan request the page makes
///    for it returns a plan.
///
/// Driven by `test.py` inside the `clickhouse/mysql-js-client` container (node:22-alpine), against
/// the `/play` page served by a real ClickHouse server, which also receives the statements. Can also be
/// run standalone against a checkout for development: node explain_harness.js programs/server/play.html
/// (the server checks are skipped without a server URL).
///
/// Usage: node explain_harness.js <path-or-url-of-play.html> [server-url]
/// Exit code 0 = all scenarios pass; 1 = failure (details on stdout).

'use strict';

const vm = require('vm');
const fs = require('fs');

function extractScript(html) {
    const blocks = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)].map(m => m[1]);
    if (!blocks.length) throw new Error('no <script> block found in play.html');
    return blocks.reduce((a, b) => (a.length >= b.length ? a : b));
}

/// The helpers under test, plus the tokenizer that feeds them. Extracted as source ranges between
/// marker declarations and evaluated into one sandbox: a refactor that moves any of them out of its
/// range fails here loudly rather than silently testing nothing.
///
/// `TT` comes first because `fallbackTokenize` reads it, and it is declared LATER in the page (the
/// page only needs it by the time anything runs). The plain-JS `fallbackTokenize` stands in for the
/// WASM lexer, which needs a browser to instantiate; both emit the same `{type, significant, token}`
/// shape tiling the input exactly, which is the only property the walk depends on.
const RANGES = [
    ['const TT = {', '/// SQL keywords recognized'],
    ['const TT_FALLBACK_OTHER = -1;', 'function fallbackTokenize('],
    ['function fallbackTokenize(', 'async function tokenizeWithFallback('],
    ['const EXPLAIN_NON_PLAN_KIND_WORDS = new Set([', 'const SETTINGS_LIST_FOLLOWERS'],
];

const HELPERS = [
    'fallbackTokenize', 'explainSettingName', 'explainSettingIsOn',
    'explainPlanRequest', 'applyExplainPlanInsertion', 'parseExplainPlan',
    'executedPlanStatement', 'executedPlanUrl', 'typedExplainPlanStatement',
];

function extractHelpers(js) {
    const sandbox = {};
    for (const [from, to] of RANGES) {
        const start = js.indexOf(from);
        const end = js.indexOf(to, start + from.length);
        if (start < 0 || end < 0) throw new Error(`marker range not found in the page script: ${from} .. ${to}`);
        vm.runInNewContext(js.slice(start, end), sandbox, { filename: 'explain_helpers.js' });
    }
    for (const name of HELPERS) {
        if (typeof sandbox[name] !== 'function') throw new Error(name + ' is missing from the extracted helpers');
    }
    return sandbox;
}

let failures = 0;

function check(scenario, what, actual, expected) {
    if (JSON.stringify(actual) === JSON.stringify(expected)) {
        console.log(`PASS [${scenario}] ${what}`);
    } else {
        failures++;
        console.log(`FAIL [${scenario}] ${what} -- actual: ${JSON.stringify(actual)}, expected: ${JSON.stringify(expected)}`);
    }
}

function main() {
    const src = process.argv[2];
    if (!src) {
        console.error('usage: node explain_harness.js <path-or-url-of-play.html>');
        process.exit(2);
    }
    const from_url = /^https?:/.test(src);
    /// The server that receives the statements: the one serving the page, unless given.
    const server = process.argv[3] || (from_url ? new URL('/', src).href : null);
    return (from_url
        ? fetch(src).then(r => {
            if (!r.ok) throw new Error(`GET ${src} -> HTTP ${r.status}`);
            return r.text();
        })
        : Promise.resolve(fs.readFileSync(src, 'utf8'))).then((html) => run(html, server));
}

async function run(html, server) {
    const H = extractHelpers(extractScript(html));

    /// One statement as the Plan view sees it: whether its plan can be drawn, and the text of the second
    /// call that fetches it (the statement as written when it sets `json` itself).
    const wire = (query) => {
        const request = H.explainPlanRequest(H.fallbackTokenize(query));
        return { plan: request.plan_json, sent: H.applyExplainPlanInsertion(query, request.insertion) };
    };

    /// A statement the page must leave byte-for-byte alone, and never read as a plan.
    const untouched = (scenario, query) => check(scenario, 'left as written', wire(query), { plan: false, sent: query });

    /// The two spellings of the kind, and the two shapes of the settings list.
    check('rewrite', 'bare EXPLAIN gets the setting', wire('EXPLAIN SELECT 1'),
        { plan: true, sent: 'EXPLAIN json = 1, indexes = 1, header = 1, actions = 1 SELECT 1' });
    check('rewrite', 'the setting lands after a spelled-out PLAN', wire('EXPLAIN PLAN SELECT 1'),
        { plan: true, sent: 'EXPLAIN PLAN json = 1, indexes = 1, header = 1, actions = 1 SELECT 1' });
    check('rewrite', 'an existing settings list keeps its entries behind a comma',
        wire('EXPLAIN PLAN indexes=1, actions=1 SELECT 1'),
        { plan: true, sent: 'EXPLAIN PLAN json = 1, header = 1, indexes=1, actions=1 SELECT 1' });
    check('rewrite', 'a one-entry list is still a list', wire('EXPLAIN header=1 SELECT 1'),
        { plan: true, sent: 'EXPLAIN json = 1, indexes = 1, actions = 1, header=1 SELECT 1' });
    check('rewrite', 'case is irrelevant', wire('explain plan select 1'),
        { plan: true, sent: 'explain plan json = 1, indexes = 1, header = 1, actions = 1 select 1' });
    check('rewrite', 'a CTE after the setting is still parsed by the server',
        wire('EXPLAIN WITH cte AS (SELECT 1) SELECT * FROM cte'),
        { plan: true, sent: 'EXPLAIN json = 1, indexes = 1, header = 1, actions = 1 WITH cte AS (SELECT 1) SELECT * FROM cte' });

    /// The user's own `json` choice wins, in both directions.
    check('user json', 'json = 1 already asks for the tree', wire('EXPLAIN PLAN json=1 SELECT 1'),
        { plan: true, sent: 'EXPLAIN PLAN json=1 SELECT 1' });
    check('user json', 'json = 0 opts back out to the indented text', wire('EXPLAIN PLAN json=0 SELECT 1'),
        { plan: false, sent: 'EXPLAIN PLAN json=0 SELECT 1' });
    check('user json', 'a boolean literal, which the server rejects, is not a tree', wire('EXPLAIN PLAN json=true SELECT 1'),
        { plan: false, sent: 'EXPLAIN PLAN json=true SELECT 1' });
    check('user json', 'a quoted setting name is the same setting', wire('EXPLAIN PLAN `json`=1 SELECT 1'),
        { plan: true, sent: 'EXPLAIN PLAN `json`=1 SELECT 1' });
    check('user json', 'a value that is not a number is not a tree',
        wire("EXPLAIN PLAN json='x' SELECT 1"), { plan: false, sent: "EXPLAIN PLAN json='x' SELECT 1" });
    check('user json', 'json behind other entries is still found',
        wire('EXPLAIN PLAN header=1, json=0 SELECT 1'), { plan: false, sent: 'EXPLAIN PLAN header=1, json=0 SELECT 1' });
    /// The detail sections the node-details pane is built from are added too, but never over an
    /// explicit choice: the pane showing less is far better than silently changing what was asked for.
    check('detail settings', 'an explicit indexes = 0 is preserved, not overridden',
        wire('EXPLAIN PLAN indexes=0 SELECT 1'),
        { plan: true, sent: 'EXPLAIN PLAN json = 1, header = 1, actions = 1, indexes=0 SELECT 1' });
    check('detail settings', 'an explicit header = 0 is preserved, not overridden',
        wire('EXPLAIN PLAN header=0 SELECT 1'),
        { plan: true, sent: 'EXPLAIN PLAN json = 1, indexes = 1, actions = 1, header=0 SELECT 1' });
    check('detail settings', 'an explicit actions = 0 is preserved, not overridden',
        wire('EXPLAIN PLAN actions=0 SELECT 1'),
        { plan: true, sent: 'EXPLAIN PLAN json = 1, indexes = 1, header = 1, actions=0 SELECT 1' });
    check('detail settings', 'a statement that already sets all three gets only json',
        wire('EXPLAIN PLAN indexes=1, header=1, actions=1 SELECT 1'),
        { plan: true, sent: 'EXPLAIN PLAN json = 1, indexes=1, header=1, actions=1 SELECT 1' });
    check('detail settings', 'a user json = 1 is respected entirely, detail settings and all',
        wire('EXPLAIN PLAN json=1 SELECT 1'),
        { plan: true, sent: 'EXPLAIN PLAN json=1 SELECT 1' });

    check('user json', 'a signed value is stepped over, not read as a name',
        wire('EXPLAIN PLAN header=+1, json=0 SELECT 1'), { plan: false, sent: 'EXPLAIN PLAN header=+1, json=0 SELECT 1' });

    /// The server rejects `json` together with `distributed = 1`, so that plan stays text.
    check('distributed', 'distributed = 1 is text only', wire('EXPLAIN distributed = 1 SELECT 1'),
        { plan: false, sent: 'EXPLAIN distributed = 1 SELECT 1' });
    check('distributed', 'distributed = 0 is planned as usual', wire('EXPLAIN distributed = 0 SELECT 1'),
        { plan: true, sent: 'EXPLAIN json = 1, indexes = 1, header = 1, actions = 1, distributed = 0 SELECT 1' });

    /// Every other kind rejects `json`, so rewriting one would break a working query.
    untouched('other kinds', 'EXPLAIN PIPELINE SELECT 1');
    untouched('other kinds', 'EXPLAIN AST SELECT 1');
    untouched('other kinds', 'EXPLAIN SYNTAX SELECT 1');
    untouched('other kinds', 'EXPLAIN QUERY TREE SELECT 1');
    untouched('other kinds', 'EXPLAIN ESTIMATE SELECT 1');
    untouched('other kinds', 'EXPLAIN TABLE OVERRIDE mysql(...)');
    untouched('other kinds', 'EXPLAIN CURRENT TRANSACTION');

    /// The SVG path: `graph = 1` belongs to PIPELINE / AST, both already out of scope by kind.
    untouched('graph', 'EXPLAIN PIPELINE graph=1 SELECT 1');
    untouched('graph', 'EXPLAIN AST graph=1 SELECT 1');

    /// Not statements at all: the walk is over tokens, so an `EXPLAIN` that is a name or text is inert.
    untouched('not a statement', 'SELECT 1');
    untouched('not a statement', 'SELECT explain FROM t');
    untouched('not a statement', "SELECT 'EXPLAIN PLAN SELECT 1'");
    untouched('not a statement', '-- EXPLAIN SELECT 1\nSELECT 1');
    untouched('not a statement', 'SELECT 1 /* EXPLAIN */');

    /// The insertion offset is a sum over the tokens, including the insignificant ones.
    check('offsets', 'a leading block comment shifts the insertion', wire('/* c */ EXPLAIN SELECT 1'),
        { plan: true, sent: '/* c */ EXPLAIN json = 1, indexes = 1, header = 1, actions = 1 SELECT 1' });
    check('offsets', 'a leading line comment shifts the insertion', wire('-- c\nEXPLAIN SELECT 1'),
        { plan: true, sent: '-- c\nEXPLAIN json = 1, indexes = 1, header = 1, actions = 1 SELECT 1' });
    check('offsets', 'newlines and tabs are counted', wire('\n\tEXPLAIN\n\tPLAN\n\tSELECT 1'),
        { plan: true, sent: '\n\tEXPLAIN\n\tPLAN json = 1, indexes = 1, header = 1, actions = 1\n\tSELECT 1' });
    check('offsets', 'a multi-byte character before the insertion does not skew it',
        wire('/* ✓éü */ EXPLAIN SELECT 1'),
        { plan: true, sent: '/* ✓éü */ EXPLAIN json = 1, indexes = 1, header = 1, actions = 1 SELECT 1' });
    check('offsets', 'degenerate input is inert', wire(''), { plan: false, sent: '' });
    check('offsets', 'a lone EXPLAIN still gets the setting', wire('EXPLAIN'),
        { plan: true, sent: 'EXPLAIN json = 1, indexes = 1, header = 1, actions = 1' });

    /// parseExplainPlan, on the shape `EXPLAIN PLAN json = 1, indexes = 1, header = 1` really returns.
    const payload = [{ Plan: {
        'Node Type': 'Limit',
        'Node Id': 'Limit_8',
        'Description': 'preliminary LIMIT',
        'Header': [{ Name: 'k', Type: 'UInt64' }],
        'Limit': 5,
        'Offset': 0,
        'With Ties': false,
        'Reads All Data': false,
        'Plans': [{
            'Node Type': 'ReadFromMergeTree',
            'Node Id': 'ReadFromMergeTree_0',
            'Indexes': [{ Type: 'MinMax', Keys: ['d'] }],
            'Expression': { Inputs: [], Actions: [{ 'Node Type': 'INPUT' }] },
        }],
    } }];
    const tree = H.parseExplainPlan(payload);
    check('parse', 'node identity', [tree.type, tree.id, tree.description],
        ['Limit', 'Limit_8', 'preliminary LIMIT']);
    check('parse', 'scalar properties are shown inline, stringified in declaration order', tree.scalars,
        [['Limit', '5'], ['Offset', '0'], ['With Ties', 'false'], ['Reads All Data', 'false']]);
    check('parse', 'array/object properties become collapsible sub-nodes', tree.structured.map(([k]) => k),
        ['Header']);
    check('parse', 'the structured value is carried through whole', tree.structured[0][1],
        [{ Name: 'k', Type: 'UInt64' }]);
    check('parse', 'Plans become children, not properties', tree.children.map(c => c.type),
        ['ReadFromMergeTree']);
    check('parse', 'a child keeps the same split', tree.children[0].structured.map(([k]) => k),
        ['Indexes', 'Expression']);
    check('parse', 'a node with only identity keys has no properties',
        [tree.children[0].scalars.length, tree.children[0].children.length], [0, 0]);

    /// The two inputs of a join, as `JoinStepLogical` describes it: `JOIN <pipeline type>`.
    const join = (description, extra = {}) => H.parseExplainPlan([{ Plan: { 'Node Type': 'Join', Description: description,
        ...extra, Plans: [{ 'Node Type': 'Expression' }, { 'Node Type': 'Expression' }] } }]).children.map(c => c.role);
    check('parse', 'a hash join names its inputs and the build side', join('JOIN FillRightFirst'), ['left', 'right · build']);
    check('parse', 'a swapped join builds from its first input', join('JOIN FillRightFirst', { Swapped: true }),
        ['left · build', 'right']);
    check('parse', 'a merge join has no build side', join('JOIN YShaped'), ['left', 'right']);
    check('parse', 'only Join inputs get a role',
        H.parseExplainPlan([{ Plan: { 'Node Type': 'Union', Plans: [{ 'Node Type': 'A' }, { 'Node Type': 'B' }] } }])
            .children.map(c => c.role), ['', '']);
    check('parse', 'the root has no role', tree.role, '');

    /// A plan node whose type is missing still renders rather than throwing.
    const nameless = H.parseExplainPlan([{ Plan: { Description: 'x' } }]);
    check('parse', 'a missing Node Type is labelled, not fatal', [nameless.type, nameless.id, nameless.description],
        ['(unknown)', '', 'x']);
    check('parse', 'a non-object entry in Plans is dropped',
        H.parseExplainPlan([{ Plan: { 'Node Type': 'A', Plans: [null, 7, { 'Node Type': 'B' }] } }])
            .children.map(c => c.type), ['B']);

    /// Everything that is not an `EXPLAIN PLAN json = 1` payload.
    for (const [what, value] of [
        ['a bare object', { Plan: {} }],
        ['an empty array', []],
        ['more than one element', [{ Plan: {} }, { Plan: {} }]],
        ['no Plan key', [{ nope: 1 }]],
        ['a non-object Plan', [{ Plan: 'x' }]],
        ['null', null],
        ['a scalar', 7],
    ]) {
        check('parse', `${what} is not a plan`, H.parseExplainPlan(value), null);
    }

    /// The `EXPLAIN` sent for an executed statement. The `FORMAT` span is what `detectExplicitFormatClause`
    /// reports in the page; here it is located by hand.
    const P = 'EXPLAIN PLAN json = 1, indexes = 1, header = 1, actions = 1 ';
    const executed = (query, format = null) => H.executedPlanStatement(H.fallbackTokenize(query),
        format === null ? null : { start: query.indexOf(format), end: query.indexOf(format) + format.length });
    check('executed', 'a SELECT is explained as written', executed('SELECT 1'), P + 'SELECT 1');
    check('executed', 'WITH', executed('WITH 1 AS x SELECT x'), P + 'WITH 1 AS x SELECT x');
    check('executed', 'FROM before SELECT', executed('FROM t SELECT a'), P + 'FROM t SELECT a');
    check('executed', 'a parenthesized UNION', executed('(SELECT 1) UNION ALL (SELECT 2)'), P + '(SELECT 1) UNION ALL (SELECT 2)');
    check('executed', 'case is irrelevant', executed('select 1'), P + 'select 1');
    check('executed', 'a trailing semicolon stays', executed('SELECT 1;'), P + 'SELECT 1;');
    check('executed', 'a leading comment stays', executed('/* c */ SELECT 1'), P + '/* c */ SELECT 1');
    check('executed', 'the FORMAT clause is removed', executed('SELECT 1 FORMAT JSON', 'FORMAT JSON'), P + 'SELECT 1 ');
    check('executed', 'a backquoted format name too', executed('SELECT 1 FORMAT `JSON`', 'FORMAT `JSON`'), P + 'SELECT 1 ');
    check('executed', 'a SETTINGS list after FORMAT stays',
        executed('SELECT 1 FORMAT JSON SETTINGS max_threads = 1', 'FORMAT JSON'), P + 'SELECT 1  SETTINGS max_threads = 1');
    check('executed', 'multi-byte text before FORMAT', executed("SELECT '✓é' FORMAT JSON", 'FORMAT JSON'), P + "SELECT '✓é' ");
    for (const query of ['INSERT INTO t SELECT 1', 'CREATE TABLE t AS SELECT 1', 'SHOW TABLES', 'SET max_threads = 1',
                         'SYSTEM FLUSH LOGS', 'EXPLAIN SELECT 1', 'DESCRIBE TABLE t', "SELECT 1 INTO OUTFILE 'x.tsv'", '', '-- c'])
        check('executed', `no plan for ${JSON.stringify(query)}`, executed(query), null);

    /// The URL of the plan request carries what changes the plan, and no query cache.
    const source = { url: 'http://h:8123/', user: 'u', database: 'db', params: { p: 'a b' }, shape: '&limit=10&page=2' };
    check('executed url', 'the run context', H.executedPlanUrl(source, 'pw'),
        'http://h:8123/?add_http_cors_header=1&default_format=JSON&framing_output_format=None&user=u&password=pw&database=db'
        + '&limit=10&page=2&param_p=a%20b');
    check('executed url', 'empty parts are left out',
        H.executedPlanUrl({ url: 'http://h/', user: '', database: '', params: {}, shape: '' }, ''),
        'http://h/?add_http_cors_header=1&default_format=JSON&framing_output_format=None');
    check('executed url', 'a server URL with a query string is continued',
        H.executedPlanUrl({ ...source, url: 'http://h/?session_id=s' }, '').startsWith('http://h/?session_id=s&add_http_cors_header=1'), true);

    /// A typed `EXPLAIN` runs as written; its Plan view makes a second call, the rewrite above minus a trailing `FORMAT`.
    const typed = (query, format = null) => H.typedExplainPlanStatement(H.fallbackTokenize(query),
        format === null ? null : { start: query.indexOf(format), end: query.indexOf(format) + format.length });
    check('typed', 'a plain EXPLAIN', typed('EXPLAIN SELECT 1'), 'EXPLAIN json = 1, indexes = 1, header = 1, actions = 1 SELECT 1');
    check('typed', 'EXPLAIN PLAN', typed('EXPLAIN PLAN SELECT 1'), 'EXPLAIN PLAN json = 1, indexes = 1, header = 1, actions = 1 SELECT 1');
    check('typed', 'the user\'s settings stay, behind ours', typed('EXPLAIN PLAN indexes = 0 SELECT 1'),
        'EXPLAIN PLAN json = 1, header = 1, actions = 1, indexes = 0 SELECT 1');
    check('typed', 'a trailing FORMAT is cut', typed('EXPLAIN SELECT 1 FORMAT TSV', 'FORMAT TSV'),
        'EXPLAIN json = 1, indexes = 1, header = 1, actions = 1 SELECT 1 ');
    for (const query of ['EXPLAIN PLAN json = 1 SELECT 1', 'EXPLAIN PLAN json = 0 SELECT 1', 'EXPLAIN distributed = 1 SELECT 1',
                         'EXPLAIN PIPELINE SELECT 1', 'EXPLAIN AST SELECT 1', 'SELECT 1', ''])
        check('typed', `no second call for ${JSON.stringify(query)}`, typed(query), null);
    const kind = (query) => H.explainPlanRequest(H.fallbackTokenize(query)).explain_plan;
    check('typed', 'every EXPLAIN PLAN is reported as one, json or not',
        ['EXPLAIN SELECT 1', 'EXPLAIN PLAN SELECT 1', 'EXPLAIN PLAN json = 1 SELECT 1', 'EXPLAIN PLAN json = 0 SELECT 1'].map(kind),
        [true, true, true, true]);
    check('typed', 'other statements are not', ['EXPLAIN PIPELINE SELECT 1', 'SELECT 1'].map(kind), [false, false]);

    if (server)
        await checkOnServer(H, server, typed, executed);
    else
        console.log('SKIP [server] no server URL, so no statement was sent');

    console.log(failures ? `\n${failures} scenario check(s) FAILED` : '\nAll scenarios passed');
    process.exit(failures ? 1 : 0);
}

/// The checks above compare strings, which cannot tell whether the server accepts them. Here each
/// statement runs on `server`, and so does the plan request the page makes for it (`executedPlanSource`).
async function checkOnServer(H, server, typed, executed) {
    const url = H.executedPlanUrl({ url: server, user: '', database: '', params: {}, shape: '' }, '');
    const post = async (statement) => {
        const response = await fetch(url, { method: 'POST', body: statement });
        const text = await response.text();
        return { ok: response.ok, text };
    };
    /// A response read as `_fetchExecutedPlan` reads it.
    const outcome = (response) => (response.ok
        ? (H.parseExplainPlan(JSON.parse(JSON.parse(response.text).data[0].explain)) !== null ? 'plan' : response.text)
        : response.text.trim());

    for (const [query, format = null] of [
        ['EXPLAIN SELECT 1'],
        ['EXPLAIN PLAN SELECT number FROM numbers(10) WHERE number > 5'],
        ['EXPLAIN PLAN indexes = 0, header = 1 SELECT 1'],
        ['EXPLAIN WITH cte AS (SELECT 1 AS x) SELECT * FROM cte'],
        ['EXPLAIN SELECT 1 FORMAT TSV', 'FORMAT TSV'],
        ['EXPLAIN distributed = 0 SELECT 1'],
        ['EXPLAIN distributed = 1 SELECT 1'],
        ['EXPLAIN PLAN json = 1 SELECT 1'],
        ['EXPLAIN PLAN json = 0 SELECT 1'],
        ['EXPLAIN PLAN header = +1, json = 0 SELECT 1'],
        ['EXPLAIN PIPELINE SELECT 1'],
        ['SELECT 1'],
        ['WITH 1 AS x SELECT x'],
        ['(SELECT 1) UNION ALL (SELECT 2)'],
        ['SELECT number FROM numbers(10) FORMAT JSON', 'FORMAT JSON'],
        ['SELECT 1 FORMAT JSON SETTINGS max_threads = 1', 'FORMAT JSON'],
    ]) {
        const response = await post(query);
        check('server', `${JSON.stringify(query)} runs`, response.ok ? 'ok' : response.text.trim(), 'ok');
        const request = H.explainPlanRequest(H.fallbackTokenize(query));
        if (request.plan_json && request.insertion === null)
            check('server', `${JSON.stringify(query)} returns its own plan`, outcome(response), 'plan');
        const plan_request = typed(query, format) ?? executed(query, format);
        if (plan_request !== null)
            check('server', `the plan request ${JSON.stringify(plan_request)} returns a plan`, outcome(await post(plan_request)), 'plan');
    }
}

main().catch((e) => {
    console.error(e);
    process.exit(2);
});
