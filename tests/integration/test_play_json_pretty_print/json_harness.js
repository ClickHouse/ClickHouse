#!/usr/bin/env node
/// Executable regression harness for the pretty-printing of a selected `JSON` cell in `/play`.
///
/// Runs the REAL `prettyPrintJSON` extracted from the served `play.html` against a minimal stand-in for
/// the three `document` methods it uses, and asserts the contracts the selected-cell view rests on:
///
///  - the layout: two-space indentation, one member or element per line, `{}` / `[]` kept compact,
///    and nothing but whitespace changed - the text of the output, stripped of the whitespace outside
///    strings, is the input stripped the same way;
///  - the value is kept verbatim: numbers beyond the precision of a double, out of its range or
///    spelled with an exponent, string escapes, repeated keys and the order of integer-like keys all
///    come out exactly as they went in (a round trip through `JSON.parse` changes every one of them);
///  - the highlighting: keys, strings, numbers and literals are the spans the stylesheet colors, and
///    the punctuation stays plain text;
///  - text that is not JSON throws instead of being shown as something else.
///
/// Driven by `test.py` inside the `clickhouse/mysql-js-client` container (node:22-alpine),
/// against the `/play` page served by a real ClickHouse server. Can also be run standalone
/// against a checkout for development: node json_harness.js programs/server/play.html
///
/// Usage: node json_harness.js <path-or-url-of-play.html>
/// Exit code 0 = all scenarios pass; 1 = failure (details on stdout).

'use strict';

const vm = require('vm');
const fs = require('fs');

function extractScript(html) {
    const blocks = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)].map(m => m[1]);
    if (!blocks.length) throw new Error('no <script> block found in play.html');
    return blocks.reduce((a, b) => (a.length >= b.length ? a : b));
}

/// Just enough of the DOM for `prettyPrintJSON`: a fragment collecting text nodes and spans.
const fakeDocument = {
    createDocumentFragment: () => ({ nodes: [], appendChild(node) { this.nodes.push(node); } }),
    createTextNode: (text) => ({ text }),
    createElement: (tag) => ({ tag, className: '', textContent: '' }),
};

function extractPrettyPrinter(js) {
    const start = js.indexOf('function prettyPrintJSON(');
    if (start < 0) throw new Error('prettyPrintJSON not found in the page script');
    /// The function ends at the first closing brace in the first column.
    const end = js.indexOf('\n}\n', start);
    if (end < 0) throw new Error('the end of prettyPrintJSON not found in the page script');
    const sandbox = { document: fakeDocument };
    vm.runInNewContext(js.slice(start, end + 3), sandbox, { filename: 'pretty_print_json.js' });
    if (typeof sandbox.prettyPrintJSON !== 'function') throw new Error('prettyPrintJSON did not evaluate to a function');
    return sandbox.prettyPrintJSON;
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

function textOf(fragment) {
    return fragment.nodes.map(node => (node.tag ? node.textContent : node.text)).join('');
}

function spansOf(fragment) {
    return fragment.nodes.filter(node => node.tag).map(node => [node.className, node.textContent]);
}

/// Drop the whitespace outside of strings, to compare two layouts of one JSON text.
function compact(json) {
    return json.replace(/"(?:[^"\\]|\\.)*"|\s+/g, m => (m[0] === '"' ? m : ''));
}

async function main() {
    const src = process.argv[2];
    if (!src) {
        console.error('usage: node json_harness.js <path-or-url-of-play.html>');
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
    const prettyPrintJSON = extractPrettyPrinter(extractScript(html));
    const pretty = (json) => textOf(prettyPrintJSON(json));

    /// Contract 1: the layout.
    check('layout', 'nested objects and arrays, two spaces per level',
        pretty('{"a":1,"b":{"c":[1,2],"d":{}},"e":[]}'),
        '{\n  "a": 1,\n  "b": {\n    "c": [\n      1,\n      2\n    ],\n    "d": {}\n  },\n  "e": []\n}');
    check('layout', 'the whitespace of the input does not matter',
        pretty(' { "a" : [ true , null ] , "b" : { } } '),
        '{\n  "a": [\n    true,\n    null\n  ],\n  "b": {}\n}');
    check('layout', 'a scalar at the top level', pretty('"x"'), '"x"');

    /// Contract 2: only the layout changes, never the value.
    for (const json of [
        '{"n":9007199254740993}',
        '{"n":1e309,"m":-1E-400,"k":1.50}',
        '{"s":"\\u00e9\\/\\n\\"","t":"é"}',
        '{"k":1,"k":2}',
        '{"b":1,"2":2,"1":3}',
        '[18446744073709551615,-9223372036854775808,0.1000000000000000055511151231257827]',
    ]) {
        check('verbatim', `${json} keeps its value`, compact(pretty(json)), json);
    }

    /// Contract 3: the highlighting.
    check('highlight', 'keys, strings, numbers and literals are spans; punctuation is not',
        spansOf(prettyPrintJSON('{"k":"v","n":-1.5e3,"a":[true,false,null]}')),
        [
            ['json-key', '"k"'], ['json-string', '"v"'],
            ['json-key', '"n"'], ['json-number', '-1.5e3'],
            ['json-key', '"a"'], ['json-literal', 'true'], ['json-literal', 'false'], ['json-literal', 'null'],
        ]);

    /// Contract 4: text that is not JSON throws.
    for (const json of ['', '{', '{"a":1,}', '[1 2]', '{"a" 1}', '01', 'nul', 'NaN', '{"a":1}x', '"\t"', '{a:1}']) {
        let threw = false;
        try { prettyPrintJSON(json); } catch (e) { threw = e instanceof SyntaxError || e.name === 'SyntaxError'; }
        check('invalid', `${JSON.stringify(json)} throws a SyntaxError`, threw, true);
    }

    if (failures) {
        console.log(`${failures} check(s) failed`);
        process.exit(1);
    }
    console.log('All scenarios passed');
}

main().catch(e => {
    console.log('ERROR: ' + (e && e.stack || e));
    process.exit(1);
});
