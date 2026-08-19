#!/usr/bin/env node
/// Executable regression harness for the `/play` result sorting.
///
/// Runs the REAL sort helpers extracted from the served `play.html` - the module-level functions
/// between `sanitizeSortColumns` and `rerunForSort`, which are pure functions of their arguments and
/// need no DOM - and asserts the contracts the column-header sort arrows rest on:
///
///  - `applySortToggle` implements the arrow semantics: an active direction deactivates, the other
///    direction replaces it, a plain click makes the column the only sort key, Shift appends after
///    the keys already in effect (or only flips the direction in place);
///  - `sortOrderExpression` builds the `ORDER BY` list of the `order` query-construction setting,
///    back-quoting every column name so an arbitrary result column name is usable as a sort key;
///  - `sortQueryKey` binds a sort to ONE statement while treating the two spellings the launch paths
///    produce for that statement (with and without its trailing `;`) as the same one, so a sort
///    survives its own re-run;
///  - `resolveSortForRun` drops a sort belonging to a different statement, in place, so the array the
///    tab shares with its result element(s) stays the same object.
///
/// Driven by `test.py` inside the `clickhouse/mysql-js-client` container (node:22-alpine),
/// against the `/play` page served by a real ClickHouse server. Can also be run standalone
/// against a checkout for development: node sort_harness.js programs/server/play.html
///
/// Usage: node sort_harness.js <path-or-url-of-play.html>
/// Exit code 0 = all scenarios pass; 1 = failure (details on stdout).

'use strict';

const vm = require('vm');
const fs = require('fs');

function extractScript(html) {
    const blocks = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)].map(m => m[1]);
    if (!blocks.length) throw new Error('no <script> block found in play.html');
    return blocks.reduce((a, b) => (a.length >= b.length ? a : b));
}

/// The sort helpers are module-level functions laid out together: from `sanitizeSortColumns` up to
/// `rerunForSort`, which is where the launch (and the DOM) begins. A refactor that moves them out of
/// that run fails here loudly rather than silently testing nothing.
function extractSortHelpers(js) {
    const start = js.indexOf('function sanitizeSortColumns(');
    const end = js.indexOf('function rerunForSort(', start);
    if (start < 0 || end < 0) throw new Error('sort helper markers not found in the page script');
    const sandbox = {};
    vm.runInNewContext(js.slice(start, end), sandbox, { filename: 'sort_helpers.js' });
    for (const name of ['sanitizeSortColumns', 'sortOrderExpression', 'sortQueryKey', 'applySortToggle', 'resolveSortForRun']) {
        if (typeof sandbox[name] !== 'function') throw new Error(`${name} is missing from the extracted helpers`);
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

async function main() {
    const src = process.argv[2];
    if (!src) {
        console.error('usage: node sort_harness.js <path-or-url-of-play.html>');
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
    const H = extractSortHelpers(extractScript(html));

    /// Contract 1: the arrow semantics of a single column.
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

    /// Contract 5: a payload read back from a URL, a history entry or a stored snapshot is validated,
    /// not trusted - it ends up in an `ORDER BY` of the next request.
    {
        check('sanitize', 'a non-array is dropped', H.sanitizeSortColumns('a'), []);
        check('sanitize', 'a missing array is dropped', H.sanitizeSortColumns(undefined), []);
        check('sanitize', 'malformed entries are dropped, valid ones kept in order',
            H.sanitizeSortColumns([null, { name: '' }, { desc: true }, { name: 'b', desc: true }, { name: 3 }, { name: 'a' }]),
            [{ name: 'b', desc: true }, { name: 'a', desc: false }]);
        check('sanitize', 'the direction is coerced to a boolean',
            H.sanitizeSortColumns([{ name: 'a', desc: 'yes' }]), [{ name: 'a', desc: true }]);
        check('sanitize', 'a duplicated column keeps only its first entry',
            H.sanitizeSortColumns([{ name: 'a', desc: true }, { name: 'a' }]), [{ name: 'a', desc: true }]);
    }

    /// Contract 6: the statement identity a sort is bound to. The two launch paths spell the same
    /// statement differently - `getQueryUnderCursor` keeps its trailing `;` and whitespace,
    /// `splitAllQueries` trims both - and a run started by a sort click goes through the latter, so
    /// the two spellings must resolve to the same statement or the sort would be dropped by its own
    /// re-run.
    {
        check('query-key', 'a trailing delimiter and whitespace do not change the statement',
            H.sortQueryKey('SELECT 1 ;\n'), H.sortQueryKey('SELECT 1'));
        check('query-key', 'repeated delimiters do not either',
            H.sortQueryKey('SELECT 1;; '), H.sortQueryKey('SELECT 1'));
        check('query-key', 'leading whitespace does not either',
            H.sortQueryKey('\n  SELECT 1'), H.sortQueryKey('SELECT 1'));
        check('query-key', 'a different statement is a different key',
            H.sortQueryKey('SELECT 1') === H.sortQueryKey('SELECT 2'), false);
        check('query-key', 'no statement resolves to the empty key', H.sortQueryKey(null), '');
        /// A `;` inside a string literal is not a trailing delimiter, so it must not be stripped.
        check('query-key', 'a semicolon inside a literal is kept', H.sortQueryKey("SELECT 'a;'"), "SELECT 'a;'");
    }

    /// Contract 7: a run applies the tab's sort only while it belongs to the statement being run, and
    /// drops it in place otherwise, so the array the tab shares with its result element(s) - which is
    /// what the header arrows read - stays the same object.
    {
        const shared = [{ name: 'a', desc: true }];
        const tab = { sortColumns: shared, sortQuery: 'SELECT a FROM t;\n' };
        check('resolve', 'the same statement, spelled by the other launch path, keeps the sort',
            H.resolveSortForRun(tab, 'SELECT a FROM t'), '`a` DESC');
        check('resolve', 'the shared array is not replaced', tab.sortColumns === shared, true);
        check('resolve', 'a different statement drops the sort', H.resolveSortForRun(tab, 'SELECT b FROM t'), '');
        check('resolve', 'the drop is in place', tab.sortColumns === shared && shared.length === 0, true);
        check('resolve', 'the sort is re-bound to the statement now running', tab.sortQuery, 'SELECT b FROM t');
        /// A tab that never sorted anything sends no `order`.
        const fresh = { sortColumns: [], sortQuery: null };
        check('resolve', 'an unsorted tab sends nothing', H.resolveSortForRun(fresh, 'SELECT 1'), '');
    }

    console.log(failures ? `${failures} check(s) failed` : 'All scenarios passed');
    process.exit(failures ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
