#!/usr/bin/env node
/// Executable regression harness for the `/play` RESULT SHAPE - the sort, the per-column filters and
/// the selected page a result is (re)produced with.
///
/// Runs the REAL shape helpers extracted from the served `play.html` - the module-level functions
/// between `sanitizeSortColumns` and `commitResultShape`, which are pure functions of their arguments
/// and need no DOM - and asserts the contracts the controls in the result view rest on:
///
///  - `applySortToggle` implements the sort-arrow semantics: an active direction deactivates, the
///    other direction replaces it, a plain click makes the column the only sort key, Shift appends
///    after the keys already in effect (or only flips the direction in place);
///  - `sortOrderExpression` and `filterExpression` build the `ORDER BY` list and the `WHERE`
///    expression of the `order` / `filter` settings, back-quoting every column name so an arbitrary
///    result column name is usable as a sort or filter key;
///  - `quoteStringLiteral` and `escapeLikePattern` turn a cell's own value into a literal that matches
///    it exactly, including a value carrying the `LIKE` metacharacters `%` and `_`;
///  - `shapeUrlParams` translates the shape into the request's construction settings, a page becoming
///    `limit` + `page`;
///  - the payloads read back from a URL, a history entry or a stored snapshot are validated, since
///    they end up in an `ORDER BY` / `WHERE` / `LIMIT` of the next request;
///  - `shapeQueryKey` binds a shape to ONE statement while treating the two spellings the launch paths
///    produce for that statement (with and without its trailing `;`) as the same one, so a shape
///    survives its own re-run;
///  - `shapeContextKey` completes that identity with the run context - the selected database, the
///    connection and the query parameters - which decides the columns just as much as the text does;
///  - `resolveShapeForRun` drops a shape belonging to a different statement, or to the same statement
///    run in a different context, in place, so the objects the tab shares with its result element(s)
///    stay the same objects;
///  - `snapshotShapeContext` re-binds a shape restored from a stored snapshot to the context the
///    snapshot recorded as producing its rows, so the first rerun after a context change drops it;
///  - `duplicateColumnNames` names the columns a header carries more than once, whose sort and filter
///    controls are not offered at all - the name-keyed `order` / `filter` settings could not tell the
///    namesakes apart;
///  - `resultIsWorthShaping` withholds the sort and filter controls from a result of at most one row
///    on its first page, where they could only re-run the query for the same rows, while keeping them
///    on the two single-row results that need them - a shaped one and one cut off at the display limit.
///
/// Driven by `test.py` inside the `clickhouse/mysql-js-client` container (node:22-alpine),
/// against the `/play` page served by a real ClickHouse server. Can also be run standalone
/// against a checkout for development: node shape_harness.js programs/server/play.html
///
/// Usage: node shape_harness.js <path-or-url-of-play.html>
/// Exit code 0 = all scenarios pass; 1 = failure (details on stdout).

'use strict';

const vm = require('vm');
const fs = require('fs');

function extractScript(html) {
    const blocks = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)].map(m => m[1]);
    if (!blocks.length) throw new Error('no <script> block found in play.html');
    return blocks.reduce((a, b) => (a.length >= b.length ? a : b));
}

/// The shape helpers are module-level functions laid out together: from `sanitizeSortColumns` up to
/// `commitResultShape`, which is where the launch (and the DOM) begins. A refactor that moves them out
/// of that run fails here loudly rather than silently testing nothing.
const HELPERS = [
    'sanitizeSortColumns', 'sanitizeFilters', 'sanitizePagination',
    'backQuoteIdentifier', 'quoteStringLiteral', 'escapeLikePattern',
    'sortOrderExpression', 'filterExpression', 'shapeQueryKey',
    'applySortToggle', 'resetPagination', 'clearResultShape',
    'shapeUrlParams', 'resolveShapeForRun', 'shapeContextKey',
    'snapshotShapeContext', 'duplicateColumnNames', 'resultIsWorthShaping',
];

function extractShapeHelpers(js) {
    const start = js.indexOf('function sanitizeSortColumns(');
    const end = js.indexOf('function commitResultShape(', start);
    if (start < 0 || end < 0) throw new Error('shape helper markers not found in the page script');
    const sandbox = {};
    vm.runInNewContext(js.slice(start, end), sandbox, { filename: 'shape_helpers.js' });
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

/// The sort as the header shows it: one entry per key, in `ORDER BY` order.
function keys(list) {
    return list.map(entry => (entry.desc ? '-' : '+') + entry.name);
}

/// A tab as the helpers see it: the three shape objects plus the statement and the run context they
/// are bound to.
function makeTab(query, context) {
    return {
        sortColumns: [], filters: {}, pagination: { page: 0, size: 0 },
        shapeQuery: query ?? null, shapeContext: context ?? null,
    };
}

async function main() {
    const src = process.argv[2];
    if (!src) {
        console.error('usage: node shape_harness.js <path-or-url-of-play.html>');
        process.exit(2);
    }
    let html;
    if (/^https?:/.test(src)) {
        const resp = await fetch(src);
        if (!resp.ok) throw new Error(`GET ${src} -> HTTP ${resp.status}`);
        html = await resp.text();
    } else {
        html = fs.readFileSync(src, 'utf8');
    }
    const H = extractShapeHelpers(extractScript(html));

    /// Contract 1: the sort-arrow semantics of a single column.
    {
        const list = [];
        H.applySortToggle(list, 'a', false, false);
        check('single-column', 'the ascending arrow activates ascending', keys(list), ['+a']);
        H.applySortToggle(list, 'a', true, false);
        check('single-column', 'the other arrow replaces the active direction', keys(list), ['-a']);
        H.applySortToggle(list, 'a', true, false);
        check('single-column', 'the active arrow deactivates the sort', keys(list), []);
    }

    /// Contract 2: a plain click makes the clicked column the ONLY sort key, whatever was sorted
    /// before - including a multi-column sort built with Shift.
    {
        const list = [];
        H.applySortToggle(list, 'a', false, false);
        H.applySortToggle(list, 'b', true, true);
        check('replace', 'Shift appends the column after the keys already in effect', keys(list), ['+a', '-b']);
        H.applySortToggle(list, 'c', false, false);
        check('replace', 'a plain click drops every other key', keys(list), ['+c']);
    }

    /// Contract 3: Shift builds and edits a multi-column sort. The ORDER of the keys is what makes
    /// "by a, then by b" different from the reverse, so a direction flip must not reorder them.
    {
        const list = [];
        H.applySortToggle(list, 'a', false, false);
        H.applySortToggle(list, 'b', false, true);
        H.applySortToggle(list, 'c', true, true);
        check('multi-column', 'the keys keep the order they were added in', keys(list), ['+a', '+b', '-c']);
        H.applySortToggle(list, 'a', true, true);
        check('multi-column', 'Shift on an active key only flips its direction, in place', keys(list), ['-a', '+b', '-c']);
        H.applySortToggle(list, 'b', false, true);
        check('multi-column', 'Shift on the active direction drops that key alone', keys(list), ['-a', '-c']);
        H.applySortToggle(list, 'b', false, false);
        check('multi-column', 'a plain click on a dropped key restarts a single-key sort', keys(list), ['+b']);
    }

    /// A deactivation without Shift also drops only the clicked column: the click says nothing about
    /// the other keys, so a multi-column sort is not wiped by it.
    {
        const list = [];
        H.applySortToggle(list, 'a', false, false);
        H.applySortToggle(list, 'b', true, true);
        H.applySortToggle(list, 'a', false, false);
        check('deactivate-no-shift', 'only the clicked column is dropped', keys(list), ['-b']);
    }

    /// Contract 4: the `ORDER BY` list sent as the `order` setting. Every column name is a quoted
    /// identifier, so a result column whose name is an expression - or carries a space, a back-quote
    /// or a backslash - is a usable sort key.
    {
        check('order-expression', 'no sort sends nothing', H.sortOrderExpression([]), '');
        check('order-expression', 'a non-array sends nothing', H.sortOrderExpression(null), '');
        check('order-expression', 'directions are explicit',
            H.sortOrderExpression([{ name: 'a', desc: false }, { name: 'b', desc: true }]), '`a` ASC, `b` DESC');
        check('order-expression', 'an expression column name is quoted',
            H.sortOrderExpression([{ name: 'count()', desc: true }]), '`count()` DESC');
        check('order-expression', 'a space in the name is quoted',
            H.sortOrderExpression([{ name: 'a b', desc: false }]), '`a b` ASC');
        check('order-expression', 'a back-quote in the name is escaped',
            H.sortOrderExpression([{ name: 'a`b', desc: false }]), '`a\\`b` ASC');
        check('order-expression', 'a backslash in the name is escaped',
            H.sortOrderExpression([{ name: 'a\\b', desc: false }]), '`a\\\\b` ASC');
    }

    /// Contract 5: the `WHERE` expression sent as the `filter` setting. Each column's predicate is
    /// parenthesized before the columns are combined with `AND`, so a suffix containing `OR` cannot
    /// swallow the neighbouring columns and turn a conjunction into a disjunction.
    {
        check('filter-expression', 'no filter sends nothing', H.filterExpression({}), '');
        check('filter-expression', 'a missing filter set sends nothing', H.filterExpression(undefined), '');
        check('filter-expression', 'the suffix follows the quoted column name',
            H.filterExpression({ price: '> 100' }), '(`price` > 100)');
        check('filter-expression', 'several columns are combined with AND',
            H.filterExpression({ price: '> 100', city: "= 'Berlin'" }), "(`price` > 100) AND (`city` = 'Berlin')");
        check('filter-expression', 'an OR inside one suffix cannot escape its column',
            H.filterExpression({ a: '= 1 OR 1', b: '= 2' }), '(`a` = 1 OR 1) AND (`b` = 2)');
        check('filter-expression', 'a blank suffix contributes nothing',
            H.filterExpression({ a: '  ', b: '= 2' }), '(`b` = 2)');
        check('filter-expression', 'an expression column name is quoted',
            H.filterExpression({ 'count()': '> 0' }), '(`count()` > 0)');
    }

    /// Contract 6: a filter built from a cell matches that cell's value and nothing else. A string
    /// literal escapes the quote and the backslash; a `LIKE` pattern additionally escapes the pattern
    /// metacharacters `%` and `_`, so a value containing them is not turned into a wildcard.
    {
        check('literals', 'a plain value is quoted', H.quoteStringLiteral('abc'), "'abc'");
        check('literals', 'a quote is escaped', H.quoteStringLiteral("a'b"), "'a\\'b'");
        check('literals', 'a backslash is escaped', H.quoteStringLiteral('a\\b'), "'a\\\\b'");
        check('literals', 'LIKE metacharacters are escaped', H.escapeLikePattern('a%b_c'), 'a\\%b\\_c');
        check('literals', 'a backslash is escaped for LIKE too', H.escapeLikePattern('a\\b'), 'a\\\\b');
        /// The two compose the way the cell menu composes them: escape for LIKE, then quote. A `%` in
        /// the value ends up as `\%` in the pattern, i.e. `\\%` in the literal.
        check('literals', 'contains on a value with a percent matches it literally',
            'LIKE ' + H.quoteStringLiteral('%' + H.escapeLikePattern('a%b') + '%'), "LIKE '%a\\\\%b%'");
    }

    /// Contract 7: the construction settings a shaped run sends. A page becomes `limit` + `page`, never
    /// an offset of its own: the server computes `limit * (page - 1)` and rejects a `page` without a
    /// `limit`, which is why an incomplete pagination is not sent at all.
    {
        const tab = makeTab('SELECT 1');
        check('url-params', 'an unshaped run sends nothing', H.shapeUrlParams(tab), '');
        tab.sortColumns.push({ name: 'a', desc: true });
        check('url-params', 'a sort sends order', H.shapeUrlParams(tab), '&order=' + encodeURIComponent('`a` DESC'));
        tab.filters.b = '> 1';
        check('url-params', 'a filter sends filter too',
            H.shapeUrlParams(tab),
            '&order=' + encodeURIComponent('`a` DESC') + '&filter=' + encodeURIComponent('(`b` > 1)'));
        tab.pagination.page = 3;
        tab.pagination.size = 1000;
        check('url-params', 'a page sends limit and page',
            H.shapeUrlParams(tab),
            '&order=' + encodeURIComponent('`a` DESC') + '&filter=' + encodeURIComponent('(`b` > 1)')
                + '&limit=1000&page=3');
        /// A page without a size cannot be turned into a request the server accepts, so it is not sent.
        const sizeless = makeTab('SELECT 1');
        sizeless.pagination.page = 2;
        check('url-params', 'a page with no size sends nothing', H.shapeUrlParams(sizeless), '');
    }

    /// Contract 8: the payloads read back from a URL, a history entry or a stored snapshot are
    /// validated, not trusted - they end up in an `ORDER BY` / `WHERE` / `LIMIT` of the next request.
    {
        check('sanitize', 'a non-array sort is dropped', H.sanitizeSortColumns('a'), []);
        check('sanitize', 'a missing sort is dropped', H.sanitizeSortColumns(undefined), []);
        check('sanitize', 'malformed sort entries are dropped, valid ones kept in order',
            H.sanitizeSortColumns([null, { name: '' }, { desc: true }, { name: 'b', desc: true }, { name: 3 }, { name: 'a' }]),
            [{ name: 'b', desc: true }, { name: 'a', desc: false }]);
        check('sanitize', 'the direction is coerced to a boolean',
            H.sanitizeSortColumns([{ name: 'a', desc: 'yes' }]), [{ name: 'a', desc: true }]);
        check('sanitize', 'a duplicated column keeps only its first entry',
            H.sanitizeSortColumns([{ name: 'a', desc: true }, { name: 'a' }]), [{ name: 'a', desc: true }]);

        check('sanitize', 'a non-object filter set is dropped', H.sanitizeFilters([1, 2]), {});
        check('sanitize', 'a missing filter set is dropped', H.sanitizeFilters(null), {});
        check('sanitize', 'non-string and blank filters are dropped, the rest kept',
            H.sanitizeFilters({ a: '> 1', b: 2, c: '', d: '   ', e: "= 'x'" }), { a: '> 1', e: "= 'x'" });

        check('sanitize', 'no pagination', H.sanitizePagination(undefined), { page: 0, size: 0 });
        check('sanitize', 'numeric strings are accepted (the URL carries them as text)',
            H.sanitizePagination({ page: '3', size: '500' }), { page: 3, size: 500 });
        check('sanitize', 'a page without a size is not a page', H.sanitizePagination({ page: 3 }), { page: 0, size: 0 });
        check('sanitize', 'a zero or negative page is not a page',
            H.sanitizePagination({ page: 0, size: 500 }), { page: 0, size: 0 });
        check('sanitize', 'a non-numeric page is not a page',
            H.sanitizePagination({ page: 'x', size: 500 }), { page: 0, size: 0 });
        /// A non-finite value passes every lower bound: an infinite page makes the pager iterate from
        /// it to it forever, and an infinite size would be sent as `limit=Infinity`. Both spellings a
        /// URL or a history entry can carry - the word and an overflowing literal - must be rejected.
        check('sanitize', 'an infinite page is not a page',
            H.sanitizePagination({ page: Infinity, size: 500 }), { page: 0, size: 0 });
        check('sanitize', 'an overflowing page literal is not a page',
            H.sanitizePagination({ page: '1e309', size: 500 }), { page: 0, size: 0 });
        check('sanitize', 'an infinite page size is not a page',
            H.sanitizePagination({ page: 2, size: Infinity }), { page: 0, size: 0 });
        check('sanitize', 'an overflowing page size literal is not a page',
            H.sanitizePagination({ page: 2, size: '1e309' }), { page: 0, size: 0 });
    }

    /// Contract 9: the statement identity a shape is bound to. The two launch paths spell the same
    /// statement differently - `getQueryUnderCursor` keeps its trailing `;` and whitespace,
    /// `splitAllQueries` trims both - and a run started from the result view goes through the latter,
    /// so the two spellings must resolve to the same statement or the shape would be dropped by its own
    /// re-run.
    {
        check('query-key', 'a trailing delimiter and whitespace do not change the statement',
            H.shapeQueryKey('SELECT 1 ;\n'), H.shapeQueryKey('SELECT 1'));
        check('query-key', 'repeated delimiters do not either',
            H.shapeQueryKey('SELECT 1;; '), H.shapeQueryKey('SELECT 1'));
        check('query-key', 'leading whitespace does not either',
            H.shapeQueryKey('\n  SELECT 1'), H.shapeQueryKey('SELECT 1'));
        check('query-key', 'a different statement is a different key',
            H.shapeQueryKey('SELECT 1') === H.shapeQueryKey('SELECT 2'), false);
        check('query-key', 'no statement resolves to the empty key', H.shapeQueryKey(null), '');
        /// A `;` inside a string literal is not a trailing delimiter, so it must not be stripped.
        check('query-key', 'a semicolon inside a literal is kept', H.shapeQueryKey("SELECT 'a;'"), "SELECT 'a;'");
    }

    /// Contract 10: changing the sort or a filter goes back to the first page, in place, since the page
    /// no longer denotes the same slice of a differently ordered or differently filtered result.
    {
        const pagination = { page: 7, size: 1000 };
        H.resetPagination(pagination);
        check('reset-pagination', 'the page and its size are cleared', pagination, { page: 0, size: 0 });
    }

    /// Contract 11: a run applies the tab's shape only while it belongs to the statement being run, and
    /// drops it in place otherwise, so the objects the tab shares with its result element(s) - which is
    /// what the header controls and the pager read - stay the same objects.
    {
        const tab = makeTab('SELECT a FROM t;\n');
        tab.sortColumns.push({ name: 'a', desc: true });
        tab.filters.a = '> 1';
        tab.pagination.page = 2;
        tab.pagination.size = 1000;
        const sort = tab.sortColumns, filters = tab.filters, pagination = tab.pagination;

        check('resolve', 'the same statement, spelled by the other launch path, keeps the shape',
            H.resolveShapeForRun(tab, 'SELECT a FROM t'),
            '&order=' + encodeURIComponent('`a` DESC') + '&filter=' + encodeURIComponent('(`a` > 1)')
                + '&limit=1000&page=2');

        check('resolve', 'a different statement drops the whole shape', H.resolveShapeForRun(tab, 'SELECT b FROM t'), '');
        check('resolve', 'the shared objects are not replaced',
            tab.sortColumns === sort && tab.filters === filters && tab.pagination === pagination, true);
        check('resolve', 'the drop is in place',
            [sort.length, Object.keys(filters).length, pagination.page, pagination.size], [0, 0, 0, 0]);
        check('resolve', 'the shape is re-bound to the statement now running', tab.shapeQuery, 'SELECT b FROM t');

        /// `clearResultShape` is the same clearing a failed run and a multi-statement run perform.
        const failed = makeTab('SELECT 1');
        failed.sortColumns.push({ name: 'a', desc: false });
        failed.filters.a = '> 1';
        failed.pagination.page = 4;
        failed.pagination.size = 100;
        H.clearResultShape(failed);
        check('resolve', 'clearResultShape empties every part of the shape',
            [failed.sortColumns.length, Object.keys(failed.filters).length, failed.pagination.page], [0, 0, 0]);
    }

    /// Contract 12: the run CONTEXT is part of what a shape is bound to. The same statement text names
    /// different columns after the selected database, the connection or a query parameter changed, so
    /// a shape carried across such a change would sort or filter a different result - by a column it
    /// may not even have.
    {
        const db_a = H.shapeContextKey('a', 'http://localhost:8123/', 'default', {});
        const db_b = H.shapeContextKey('b', 'http://localhost:8123/', 'default', {});
        check('context-key', 'the same database, connection and parameters are the same context',
            H.shapeContextKey('a', 'http://localhost:8123/', 'default', { tbl: 'hits' }),
            H.shapeContextKey('a', 'http://localhost:8123/', 'default', { tbl: 'hits' }));
        check('context-key', 'a different database is a different context', db_a === db_b, false);
        check('context-key', 'a different server is a different context',
            db_a === H.shapeContextKey('a', 'http://other:8123/', 'default', {}), false);
        check('context-key', 'a different user is a different context',
            db_a === H.shapeContextKey('a', 'http://localhost:8123/', 'reader', {}), false);
        check('context-key', 'a different parameter value is a different context',
            H.shapeContextKey('a', 'u', 'd', { tbl: 'hits' }) === H.shapeContextKey('a', 'u', 'd', { tbl: 'visits' }), false);
        /// The parameters are a set of bindings, not an ordered list: rebuilding the same bindings in
        /// another order (which `extractRunParamNames` can) must not drop the shape.
        check('context-key', 'the parameter order does not matter',
            H.shapeContextKey('a', 'u', 'd', { x: '1', y: '2' }), H.shapeContextKey('a', 'u', 'd', { y: '2', x: '1' }));

        const tab = makeTab('SELECT * FROM events', db_a);
        tab.sortColumns.push({ name: 'a', desc: false });
        check('context', 'the same statement in the same context keeps the shape',
            H.resolveShapeForRun(tab, 'SELECT * FROM events', db_a), '&order=' + encodeURIComponent('`a` ASC'));
        check('context', 'the same statement in another database drops the shape',
            H.resolveShapeForRun(tab, 'SELECT * FROM events', db_b), '');
        check('context', 'the shape is re-bound to the context now running', tab.shapeContext, db_b);

        /// A shape restored from a URL carries no context of its own (a link records no producing
        /// database, connection or parameters): it adopts the one of the run that re-applies it -
        /// for a `run=1` link, the auto-run that follows the load - and a change AFTER that drops it.
        const restored = makeTab('SELECT * FROM events', null);
        restored.filters.a = '> 1';
        check('context', 'a URL-restored (unbound) shape survives the run that adopts its context',
            H.resolveShapeForRun(restored, 'SELECT * FROM events', db_a),
            '&filter=' + encodeURIComponent('(`a` > 1)'));
        check('context', 'the adopted context is recorded', restored.shapeContext, db_a);
        check('context', 'and a change after it drops the shape',
            H.resolveShapeForRun(restored, 'SELECT * FROM events', db_b), '');
    }

    /// Contract 13: a shape restored from a stored snapshot (a reload, a tab reopened from IndexedDB,
    /// Back/Forward) is NOT unbound: the snapshot records the database, connection and parameters that
    /// produced its rows, and the restored shape is re-bound to that context, so the FIRST rerun after
    /// a context change already drops it instead of adopting the changed context and shaping a
    /// different result.
    {
        const snapshot = {
            query: 'SELECT * FROM events',
            database: 'a', url: 'http://localhost:8123/', user: 'default', params: {},
        };
        const producing = H.shapeContextKey('a', 'http://localhost:8123/', 'default', {});
        check('snapshot-context', 'a restored snapshot is bound to the context that produced it',
            H.snapshotShapeContext(snapshot), producing);

        const reloaded = makeTab(snapshot.query, H.snapshotShapeContext(snapshot));
        reloaded.sortColumns.push({ name: 'EventTime', desc: false });
        check('snapshot-context', 'rerunning the statement in the producing context keeps the shape',
            H.resolveShapeForRun(reloaded, 'SELECT * FROM events', producing),
            '&order=' + encodeURIComponent('`EventTime` ASC'));
        check('snapshot-context', 'a database change before the first rerun drops the shape',
            H.resolveShapeForRun(reloaded, 'SELECT * FROM events',
                H.shapeContextKey('b', 'http://localhost:8123/', 'default', {})), '');

        /// The trace that motivates the binding: `SELECT * FROM {tbl:Identifier}` snapshotted sorted
        /// by a column of `hits`, restored, `tbl` edited to `visits`, rerun - the old `ORDER BY` must
        /// not be sent into a different table's result.
        const param_snapshot = {
            query: 'SELECT * FROM {tbl:Identifier}',
            database: 'a', url: 'u', user: 'd', params: { tbl: 'hits' },
        };
        const param_tab = makeTab(param_snapshot.query, H.snapshotShapeContext(param_snapshot));
        param_tab.sortColumns.push({ name: 'EventTime', desc: true });
        check('snapshot-context', 'a parameter edit before the first rerun drops the shape',
            H.resolveShapeForRun(param_tab, 'SELECT * FROM {tbl:Identifier}',
                H.shapeContextKey('a', 'u', 'd', { tbl: 'visits' })), '');

        /// A snapshot that never recorded a producing query has nothing to bind to - and no shape
        /// either, a shape being made on a produced result.
        check('snapshot-context', 'a snapshot without a producing query stays unbound',
            H.snapshotShapeContext({ data: '1' }), null);
        check('snapshot-context', 'a missing snapshot stays unbound', H.snapshotShapeContext(null), null);
    }

    /// Contract 14: a result can legitimately carry one column name twice (`SELECT 1 AS x, 2 AS x`, a
    /// join of tables sharing column names, a `SELECT *` expansion), and the name-keyed `order` /
    /// `filter` settings could not tell the namesakes apart, so their sort and filter controls are not
    /// offered at all - fail closed - while the uniquely named columns keep theirs.
    {
        const dup = H.duplicateColumnNames([{ name: 'x' }, { name: 'y' }, { name: 'x' }]);
        check('duplicate-columns', 'a repeated name is recognized', [...dup], ['x']);
        check('duplicate-columns', 'a unique name among duplicates is not', dup.has('y'), false);
        check('duplicate-columns', 'a header of unique names has no duplicates',
            [...H.duplicateColumnNames([{ name: 'x' }, { name: 'y' }])], []);
        check('duplicate-columns', 'a missing header has none', [...H.duplicateColumnNames(undefined)], []);
        check('duplicate-columns', 'a name repeated more than twice is recorded once',
            [...H.duplicateColumnNames([{ name: 'x' }, { name: 'x' }, { name: 'x' }])], ['x']);
    }

    /// Contract 15: the sort and filter controls are withheld from a result of at most one row on its
    /// first page - any order of one row is the same order, and a filter on it can only keep it or drop
    /// it - but not from the two single-row results where they are the only way back: one the user has
    /// already shaped (its sort must stay reversible and its filter clearable) and one cut off at the
    /// display limit (the first page of a longer result, which a very wide result can be cut to).
    {
        check('worth-shaping', 'many rows are worth shaping', H.resultIsWorthShaping(1000, false, false), true);
        check('worth-shaping', 'two rows are', H.resultIsWorthShaping(2, false, false), true);
        check('worth-shaping', 'a single row is not', H.resultIsWorthShaping(1, false, false), false);
        check('worth-shaping', 'an empty result is not', H.resultIsWorthShaping(0, false, false), false);
        check('worth-shaping', 'a single row of a shaped result is',
            H.resultIsWorthShaping(1, false, true), true);
        check('worth-shaping', 'an empty shaped result is - its filter must stay clearable',
            H.resultIsWorthShaping(0, false, true), true);
        check('worth-shaping', 'a single row cut off at the display limit is',
            H.resultIsWorthShaping(1, true, false), true);
    }

    console.log(failures ? `${failures} check(s) failed` : 'All scenarios passed');
    process.exit(failures ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
