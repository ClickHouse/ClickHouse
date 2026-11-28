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
///  - text that is not JSON throws instead of being shown as something else;
///  - selecting a `JSON` cell does not change its category: with categorical coloring, equal values
///    get equal hues and the selected cell's peers are emphasized whether the coloring is enabled
///    before or after the selection, and moving or clearing the selection or repainting the column
///    keeps that consistent.
///    This runs the REAL selection, coloring and emphasis methods of `QueryResultElement` against
///    a minimal stand-in for the table cells.
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

/// The text of a node of the stand-in DOM below: a fragment, a text node or an element.
function nodeText(node) {
    if (node.nodes) return node.nodes.map(nodeText).join('');
    return node.tag ? node.textContent : node.text;
}

/// Just enough of the DOM for `prettyPrintJSON` and `_expandJSON`: a fragment collecting text nodes
/// and spans, and elements whose text is what was appended to them.
const fakeDocument = {
    createDocumentFragment: () => ({ nodes: [], appendChild(node) { this.nodes.push(node); } }),
    createTextNode: (text) => ({ text }),
    createElement: (tag) => ({ tag, className: '', textContent: '', appendChild(node) { this.textContent += nodeText(node); } }),
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

/// Extract methods of `QueryResultElement` from the page script, as an object of functions to be
/// bound to a stand-in for the element. A method starts at four spaces of indentation and ends at
/// the first closing brace at the same indentation.
function extractResultMethods(js, names, sandbox) {
    const class_start = js.indexOf('\nclass QueryResultElement ');
    if (class_start < 0) throw new Error('QueryResultElement not found in the page script');
    const sources = names.map(name => {
        const start = js.indexOf(`\n    ${name}(`, class_start);
        if (start < 0) throw new Error(`QueryResultElement.${name} not found in the page script`);
        const end = js.indexOf('\n    }\n', start);
        if (end < 0) throw new Error(`the end of QueryResultElement.${name} not found in the page script`);
        return js.slice(start, end + 6);
    });
    return vm.runInNewContext(`({${sources.join(',')}})`, sandbox, { filename: 'query_result_methods.js' });
}

/// A stand-in for a `<td>` of the result table: its `.cell-content` holds the rendered text, the
/// way `_renderCell` leaves it, and `innerText` reads back whatever is displayed now.
function makeCell(value, is_json) {
    const classes = () => { const set = new Set(); return { add: c => set.add(c), remove: c => set.delete(c) }; };
    const content = {
        children: [fakeDocument.createTextNode(value === null ? 'ᴺᵁᴸᴸ' : value)],
        style: {},
        replaceChildren(...nodes) { this.children = nodes; },
        get innerText() { return this.children.map(nodeText).join(''); },
        scrollIntoView() {},
    };
    const td = {
        cellIndex: 1,
        dataset: {},
        style: { background: '', setProperty() {}, removeProperty() {} },
        classList: classes(),
        parentElement: { classList: classes() },
        content,
        querySelector: (selector) => (selector === '.cell-content' ? content : null),
        focus() {},
        removeAttribute() {},
    };
    if (value === null) td.dataset.isNull = '';
    else td._filterText = value;
    if (is_json && value !== null) td._jsonText = value;
    return td;
}

/// Contract 5: selecting a `JSON` cell does not change its category for categorical coloring.
function checkCategoricalSelection(js, prettyPrintJSON) {
    const sandbox = { document: fakeDocument, prettyPrintJSON };
    const methods = extractResultMethods(js, [
        '_selectCell', 'clearSelection', '_expandJSON', '_collapseJSON',
        '_hashHue', '_clearColumnColor', '_applyColumnColor', '_categoricalCellText',
        '_setCellEmphasis', '_clearCategoricalEmphasis', '_refreshCategoricalEmphasis',
        'applyColumnColors',
    ], sandbox);

    const A = '{"a":[1,2],"b":{"c":"x"}}';
    const B = '{"a":[1,3]}';
    const values = [A, A, B, null, A];
    const rows = values.map(value => ({ cells: [{}, makeCell(value, true)] }));
    const cells = rows.map(row => row.cells[1]);

    let mode = 'none';
    const el = Object.assign(Object.create(null), {
        _header: [{ name: 'j' }],
        _dataTable: { tHead: {} },
        _current_selected_cell: null,
        _emphasized_col_idx: null,
        _emphasized_value: null,
        _emphasized_active_cell: null,
        _colorTargetRows: () => rows,
        _effectiveColorMode: () => mode,
        _removeCellFilterControls() {},
        _appendCellFilterButton() {},
        _positionCellControls() {},
        _updateColorizeToggleVisibility() {},
        _willTranspose: () => false,
        applyPinnedColumns() {},
        /// Synchronously, rather than after the 100 ms quiet period of the page.
        _scheduleCategoricalEmphasis() { this._refreshCategoricalEmphasis(); },
    });
    for (const [name, method] of Object.entries(methods)) el[name] = method;

    /// What `refreshColumnColor` does when the categorical toggle of the column is clicked.
    const setMode = (new_mode) => { mode = new_mode; el._applyColumnColor('j', 1); el._scheduleCategoricalEmphasis(); };
    /// Repaint the column the way the page does: toggling the coloring off and on again.
    const recolor = () => { setMode('none'); setMode('categorical'); };
    const hues = () => cells.map(c => c.style.background || '');
    const emphasized = () => cells.map(c => c.content.style.fontWeight === '600');
    const hueOf = (value) => hues()[values.indexOf(value)];

    const checkHues = (scenario) => {
        const h = hues();
        check(scenario, 'equal values have one hue', [h[1], h[4]], [h[0], h[0]]);
        check(scenario, 'different values have different hues', h[0] !== h[2] && h[0] !== '' && h[2] !== '', true);
        check(scenario, 'NULL is not colored', h[3], '');
    };

    /// Select first, then enable the coloring: the hue of the selected cell is that of its value.
    el._selectCell(cells[0]);
    check('select-then-color', 'the selected cell is pretty-printed', cells[0].content.innerText.includes('\n'), true);
    setMode('categorical');
    checkHues('select-then-color');
    check('select-then-color', 'the peers of the selected cell are emphasized', emphasized(), [false, true, false, false, true]);

    /// Enable the coloring first, then select.
    el.clearSelection();
    setMode('none');
    setMode('categorical');
    const hues_unselected = hues();
    el._selectCell(cells[0]);
    recolor();
    check('color-then-select', 'selecting keeps the hues', hues(), hues_unselected);
    checkHues('color-then-select');
    check('color-then-select', 'the peers of the selected cell are emphasized', emphasized(), [false, true, false, false, true]);

    /// Move the selection within the same value, then to another value.
    el._selectCell(cells[1]);
    check('move-selection', 'to a peer: the previous cell becomes an emphasized peer', emphasized(), [true, false, false, false, true]);
    check('move-selection', 'the previous cell is collapsed back to the compact form', cells[0].content.innerText, A);
    el._selectCell(cells[4]);
    check('move-selection', 'to another peer', emphasized(), [true, true, false, false, false]);
    el._selectCell(cells[2]);
    check('move-selection', 'to a value without peers: nothing is emphasized', emphasized(), [false, false, false, false, false]);
    recolor();
    check('move-selection', 'the hue of the newly selected cell is kept', hueOf(B), hues_unselected[2]);

    /// Clear the selection.
    el._selectCell(cells[0]);
    el.clearSelection();
    check('clear-selection', 'nothing is emphasized', emphasized(), [false, false, false, false, false]);
    check('clear-selection', 'the cell is collapsed back to the compact form', cells[0].content.innerText, A);
    recolor();
    check('clear-selection', 'the hues are kept', hues(), hues_unselected);

    /// Repaint the column directly, the way `postImpl` does when the query completes, without
    /// switching the coloring off first: the emphasis of the peers must survive the repaint.
    el._selectCell(cells[0]);
    check('repaint', 'the peers are emphasized before the repaint', emphasized(), [false, true, false, false, true]);
    el.applyColumnColors();
    check('repaint', 'the hues are kept', hues(), hues_unselected);
    check('repaint', 'the peers are emphasized after the repaint', emphasized(), [false, true, false, false, true]);
    el.applyColumnColors();
    el._selectCell(cells[1]);
    check('repaint', 'moving to a peer after the repaint emphasizes the whole group', emphasized(), [true, false, false, false, true]);
    el.clearSelection();
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
    const js = extractScript(html);
    const prettyPrintJSON = extractPrettyPrinter(js);
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

    /// Contract 5: selecting a `JSON` cell does not change its category.
    checkCategoricalSelection(js, prettyPrintJSON);

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
