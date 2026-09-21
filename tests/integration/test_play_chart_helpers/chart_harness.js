#!/usr/bin/env node
/// Executable regression harness for the `/play` chart data preparation.
///
/// Runs the REAL chart helpers extracted from the served `play.html` - the static methods of
/// `QueryResultElement` between the `_CHART_TIME_*` constants and `renderChart`, which are pure
/// functions of their arguments and need no DOM - and asserts the contracts of `_prepareChartData`:
///
///  - a result ordered by x descending is reordered ascending, so the hover balloon binary search
///    finds the point under the cursor instead of a mirrored one; this includes quoted `Int64` /
///    `UInt64` x values above 2^53, whose neighbouring values collapse into the same double;
///  - `Date`, `Date32`, `DateTime` and `DateTime64` strings become Unix timestamps, and format
///    back in ISO 8601 with exactly as much of the time of day as the data carries - a
///    `DateTime64` keeps the digits of its scale, up to the microseconds a double can hold;
///  - rows with a null x are dropped, a null y is kept as a gap.
///
/// Driven by `test.py` inside the `clickhouse/mysql-js-client` container (node:22-alpine),
/// against the `/play` page served by a real ClickHouse server. Can also be run standalone
/// against a checkout for development: node chart_harness.js programs/server/play.html
///
/// Usage: node chart_harness.js <path-or-url-of-play.html>
/// Exit code 0 = all scenarios pass; 1 = failure (details on stdout).

'use strict';

const vm = require('vm');
const fs = require('fs');

function extractScript(html) {
    const blocks = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)].map(m => m[1]);
    if (!blocks.length) throw new Error('no <script> block found in play.html');
    return blocks.reduce((a, b) => (a.length >= b.length ? a : b));
}

/// The chart helpers are static methods with no DOM or uPlot dependency, laid out together in the
/// class: from the `_CHART_TIME_*` constants up to `renderChart`, which is where the rendering
/// (and the DOM) begins. A refactor that moves them fails here loudly rather than silently
/// testing nothing.
function extractChartHelpers(js) {
    const start = js.indexOf('static _CHART_TIME_DATE');
    const end = js.indexOf('async renderChart', start);
    if (start < 0 || end < 0) throw new Error('chart helper markers not found in the page script');
    return vm.runInNewContext(
        '(class QueryResultElement {\n' + js.slice(start, end) + '\n})',
        {}, { filename: 'chart_helpers.js' });
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

async function main() {
    const src = process.argv[2];
    if (!src) {
        console.error('usage: node chart_harness.js <path-or-url-of-play.html>');
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
    const Q = extractChartHelpers(extractScript(html));

    /// Contract 1: a descending result is reordered ascending, keeping x and y paired, so the
    /// balloon shows the point under the cursor (the original mirrored-hover bug).
    {
        const r = Q._prepareChartData([[1700000300, 1700000200, 1700000100], [3, 2, 1]]);
        check('desc-x', 'x ascending', r.data[0], [1700000100, 1700000200, 1700000300]);
        check('desc-x', 'y follows x', r.data[1], [1, 2, 3]);
        check('desc-x', 'the 1e9..2e9 heuristic keeps the time scale', r.x_is_time, true);
    }

    /// Contract 2: quoted 64-bit integers above 2^53 are ordered losslessly. Both neighbours
    /// round to the same double, so a `Number`-based comparison would pass this descending pair
    /// off as already ordered.
    {
        const r = Q._prepareChartData([['9223372036854775807', '9223372036854775806'], [1, 2]]);
        check('int64-x', 'descending pair above 2^53 is reordered',
            r.data[0], ['9223372036854775806', '9223372036854775807']);
        check('int64-x', 'y follows x', r.data[1], [2, 1]);
        check('int64-x', 'negative Int64 neighbours compare exactly',
            Q._compareChartX('-9223372036854775808', '-9223372036854775807'), -1);
        check('int64-x', 'plain numbers still compare numerically', Q._compareChartX(2, 10), -1);
        check('int64-x', 'decimal strings still compare numerically', Q._compareChartX('1.5', '10.5'), -1);
    }

    /// Contract 3: a `DateTime64` keeps the digits of its scale in the balloon and on the axis.
    /// `toISOString` alone stops at milliseconds and would render `.123456` back as `.123`.
    {
        const r = Q._prepareChartData([['2026-08-11 12:34:56.123456', '2026-08-11 12:34:57.000001'], [1, 2]]);
        check('dt64-6', 'x is a time scale', r.x_is_time, true);
        check('dt64-6', 'balloon keeps all six digits',
            Q._formatChartTime(r.data[0][0], r.x_precision), '2026-08-11 12:34:56.123456');
        check('dt64-6', 'trailing zeroes of the scale are kept',
            Q._formatChartTime(r.data[0][1], r.x_precision), '2026-08-11 12:34:57.000001');
        check('dt64-6', 'sub-millisecond axis ticks keep the digits too',
            Q._chartTimeAxisValues(null, r.data[0], 0, 0, 0.000001)[0], '2026-08-11\n12:34:56.123456');
    }

    /// A scale beyond microseconds is capped: at the present epoch a double resolves fractions
    /// of a microsecond, so nanosecond digits would be noise.
    {
        const r = Q._prepareChartData([['2026-08-11 12:34:56.123456789'], [1]]);
        check('dt64-9', 'nanoseconds are capped at microseconds',
            Q._formatChartTime(r.data[0][0], r.x_precision), '2026-08-11 12:34:56.123457');
    }

    /// A scale below milliseconds shows exactly its own digits, no padding to three.
    {
        const r = Q._prepareChartData([['2026-08-11 12:34:56.7'], [1]]);
        check('dt64-1', 'one digit of scale, one digit shown',
            Q._formatChartTime(r.data[0][0], r.x_precision), '2026-08-11 12:34:56.7');
    }

    /// Contract 4: the other date and time flavours format back exactly as the server wrote them.
    {
        const r = Q._prepareChartData([['2026-08-12', '2026-08-11'], [2, 1]]);
        check('date', 'descending dates are reordered', r.data[1], [1, 2]);
        check('date', 'a Date needs no time of day',
            Q._formatChartTime(r.data[0][0], r.x_precision), '2026-08-11');
    }
    {
        const r = Q._prepareChartData([['2026-08-11 12:34:56'], [1]]);
        check('datetime', 'a DateTime stops at seconds',
            Q._formatChartTime(r.data[0][0], r.x_precision), '2026-08-11 12:34:56');
        check('datetime', 'a newline separator stacks the time under the date',
            Q._formatChartTime(r.data[0][0], r.x_precision, '\n'), '2026-08-11\n12:34:56');
    }
    {
        const r = Q._prepareChartData([['1969-12-31 23:59:59.1234'], [1]]);
        check('pre-1970', 'a negative timestamp keeps its fraction',
            Q._formatChartTime(r.data[0][0], r.x_precision), '1969-12-31 23:59:59.1234');
    }
    {
        const r = Q._prepareChartData([['2026-08-11T12:34:56.123456+03:00'], [1]]);
        check('iso-offset', 'an explicit offset is converted to UTC, digits intact',
            Q._formatChartTime(r.data[0][0], r.x_precision), '2026-08-11 09:34:56.123456');
    }

    /// Contract 5: a row with a null x is dropped, a null y is kept and drawn as a gap.
    {
        const r = Q._prepareChartData([['2026-08-11', null, '2026-08-13'], [1, 2, null]]);
        check('nulls', 'the null-x row is dropped', r.data[0].length, 2);
        check('nulls', 'the null y is kept as a gap', r.data[1], [1, null]);
    }

    console.log(failures ? `${failures} check(s) failed` : 'All scenarios passed');
    process.exit(failures ? 1 : 0);
}

main().catch((e) => { console.error(e); process.exit(1); });
