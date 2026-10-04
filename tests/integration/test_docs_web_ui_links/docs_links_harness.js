#!/usr/bin/env node
/// Executable regression harness for the link rewriting of the built-in `/docs` page.
///
/// Runs the REAL code extracted from the served `docs.html` - the contiguous block of pure
/// helpers from `DOCS_SECTION_TYPE` up to `DOCS_URL_COMPATIBILITY_CASES` (`typeFromDocsHref`,
/// `resolveDocEntity`, `linkToEntity`, `rewriteLinks`, `candidateTerm`, `toDocsURL`), plus
/// `stateToHash` - against the real `system.documentation` corpus of the server, and drives
/// `rewriteLinks` through a minimal DOM shim, clicking each link and asserting which
/// navigation the click performs. The contracts pinned here:
///
///  - a bare fragment whose id is NOT an anchor of the rendered document, but names another
///    documented entity (e.g. the `#query_plan_max_limit_for_top_k_optimization` link inside
///    `enable_group_by_top_k_optimization`), becomes an in-app link: clicking it opens that
///    entity (`openTerm`) instead of only mutating `section=` in the URL - the original bug;
///  - a bare fragment whose id IS an anchor of the rendered document stays an in-page anchor
///    (`openSection`), even when the id also names a documented entity;
///  - the "#" heading anchors are left alone: their href is a whole app state hash, and the
///    fragment handling used to mistake the encoded state for an element id and push a
///    corrupted `section=q%3D...` URL;
///  - an ambiguous fragment (a name documented under several types, with no route to
///    disambiguate) is not guessed: it stays an in-page anchor;
///  - a fragment with no in-page target and no documented entity (a heading that exists only
///    on the website's combined pages) also stays an in-page anchor;
///  - a relative link to a documented entity still opens it in the app, and an absolute link
///    is left as an external link opening in a new tab.
///
/// Driven by `test.py` inside the `clickhouse/mysql-js-client` container (node:22-alpine),
/// against the `/docs` page served by a real ClickHouse server:
///     node docs_links_harness.js http://host:8123
/// Can also be run standalone against a checkout for development (a built-in fixture replaces
/// the server corpus): node docs_links_harness.js programs/server/docs.html
///
/// Exit code 0 = all scenarios pass; 1 = failure (details on stdout).

'use strict';

const vm = require('vm');
const fs = require('fs');

function extractScript(html) {
    const blocks = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)].map(m => m[1]);
    if (!blocks.length) throw new Error('no <script> block found in docs.html');
    return blocks.reduce((a, b) => (a.length >= b.length ? a : b));
}

/// The link helpers are top-level functions of the page script with no DOM dependency of their
/// own (the DOM comes in as the `body` argument), laid out together: from the
/// `DOCS_SECTION_TYPE` table up to `DOCS_URL_COMPATIBILITY_CASES`, where the self-checks begin.
/// `stateToHash` lives in the state block and is extracted separately, by its neighbor
/// `readStateFromURL`. A refactor that moves them fails here loudly rather than silently
/// testing nothing.
function extractSlice(js, startMarker, endMarker) {
    const start = js.indexOf(startMarker);
    const end = js.indexOf(endMarker, start);
    if (start < 0 || end < 0) throw new Error(`markers not found in the page script: ${startMarker} .. ${endMarker}`);
    return js.slice(start, end);
}

function extractLinkHelpers(js, sandbox) {
    const code = extractSlice(js, 'const DOCS_SECTION_TYPE', 'const DOCS_URL_COMPATIBILITY_CASES')
        + '\n' + extractSlice(js, 'function stateToHash', 'function readStateFromURL')
        + '\n;({ rewriteLinks, resolveDocEntity, candidateTerm, toDocsURL, stateToHash })';
    return vm.runInNewContext(code, sandbox, { filename: 'docs_link_helpers.js' });
}

/* --------------------------------------------------------------------------------------------
   Minimal DOM shim: just enough for `rewriteLinks(body)` - a list of anchors with attributes,
   classes and click listeners, and a body that can be asked for the anchors and for an
   element id.
   -------------------------------------------------------------------------------------------- */

class FakeAnchor {
    constructor(href, text, classes = []) {
        this.attrs = new Map(href === null ? [] : [['href', href]]);
        this.classes = new Set(classes);
        this.textContent = text || '';
        this.listeners = [];
        this.classList = {
            add: c => this.classes.add(c),
            contains: c => this.classes.has(c),
        };
    }
    get href() { return this.attrs.get('href'); }
    set href(v) { this.attrs.set('href', v); }
    getAttribute(name) { return this.attrs.has(name) ? this.attrs.get(name) : null; }
    setAttribute(name, value) { this.attrs.set(name, String(value)); }
    addEventListener(type, fn) { if (type === 'click') this.listeners.push(fn); }
    /// Dispatch a click; returns whether some listener called `preventDefault` (i.e. the
    /// browser would NOT follow the href).
    click() {
        let prevented = false;
        const event = { preventDefault() { prevented = true; } };
        for (const fn of this.listeners) fn(event);
        return prevented;
    }
}

function fakeBody(anchors, ids) {
    return {
        querySelectorAll(selector) {
            if (selector !== 'a') throw new Error(`unexpected querySelectorAll(${selector})`);
            return anchors;
        },
        /// `rewriteLinks` probes for an in-page anchor with `[id="${CSS.escape(id)}"]`.
        querySelector(selector) {
            const m = /^\[id="([\s\S]*)"\]$/.exec(selector);
            if (!m) throw new Error(`unexpected querySelector(${selector})`);
            const id = m[1].replace(/\\([\s\S])/g, '$1');
            return ids.has(id) ? { id } : null;
        },
    };
}

/* --------------------------------------------------------------------------------------------
   Corpus: the same `name -> [{name, type}]` map the page builds in `loadAllNames`, fetched
   from the real server in the integration test, or a small fixture in standalone mode.
   -------------------------------------------------------------------------------------------- */

async function loadNamesFromServer(base) {
    const response = await fetch(base + '/?default_format=JSONEachRow', {
        method: 'POST',
        body: 'SELECT name, type FROM system.documentation',
    });
    if (!response.ok) throw new Error(`loading system.documentation -> HTTP ${response.status}: ${await response.text()}`);
    const map = new Map();
    for (const line of (await response.text()).split('\n')) {
        if (!line) continue;
        const row = JSON.parse(line);
        const key = row.name.toLowerCase();
        let list = map.get(key);
        if (!list) { list = []; map.set(key, list); }
        list.push(row);
    }
    return map;
}

function fixtureNames() {
    const map = new Map();
    map.set('query_plan_max_limit_for_top_k_optimization',
        [{ name: 'query_plan_max_limit_for_top_k_optimization', type: 'Setting' }]);
    map.set('enable_group_by_top_k_optimization',
        [{ name: 'enable_group_by_top_k_optimization', type: 'Setting' }]);
    map.set('file', [{ name: 'file', type: 'Table Function' }, { name: 'file', type: 'Table Engine' }]);
    map.set('json', [{ name: 'JSON', type: 'Data Type' }, { name: 'JSON', type: 'Format' }]);
    return map;
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
        console.error('usage: node docs_links_harness.js <server-base-url-or-path-of-docs.html>');
        process.exit(2);
    }
    let html;
    let all_names;
    if (/^https?:/.test(src)) {
        const base = src.replace(/\/+$/, '');
        const resp = await fetch(base + '/docs');
        if (!resp.ok) throw new Error(`GET ${base}/docs -> HTTP ${resp.status}`);
        html = await resp.text();
        all_names = await loadNamesFromServer(base);
    } else {
        html = fs.readFileSync(src, 'utf8');
        all_names = fixtureNames();
    }

    /// The context the extracted helpers close over: the corpus, the search box, the two
    /// navigation entry points (recorded per scenario), and `CSS.escape` (absent in Node).
    const calls = [];
    const sandbox = {
        all_names,
        $search: { value: '' },
        openTerm: entity => calls.push({ fn: 'openTerm', name: entity.name, type: entity.type }),
        openSection: id => calls.push({ fn: 'openSection', id }),
        CSS: { escape: s => String(s).replace(/[^a-zA-Z0-9_-]/g, c => '\\' + c) },
        URLSearchParams,
        console,
    };
    const H = extractLinkHelpers(extractScript(html), sandbox);

    function rewriteOne(anchor, ids = []) {
        calls.length = 0;
        H.rewriteLinks(fakeBody([anchor], new Set(ids)));
        return anchor;
    }

    /// Scenario 1: the original bug. Inside `enable_group_by_top_k_optimization` the description
    /// links to `[query_plan_max_limit_for_top_k_optimization](#query_plan_max_limit_for_top_k_optimization)`.
    /// The rendered document has no such anchor, so the link must open that setting in the app
    /// instead of doing nothing (the pre-fix behavior only appended `&section=...` to the URL).
    {
        const target = 'query_plan_max_limit_for_top_k_optimization';
        check('cross-entity-fragment', 'the regression target is documented as a unique Setting',
            (all_names.get(target) || []).map(e => e.type), ['Setting']);
        const a = rewriteOne(new FakeAnchor('#' + target, target), ['settings-overview']);
        check('cross-entity-fragment', 'the link becomes an in-app entity link', a.classList.contains('doc-internal-link'), true);
        check('cross-entity-fragment', 'the href carries the entity app state', a.href, '#name=' + target + '&type=Setting');
        const prevented = a.click();
        check('cross-entity-fragment', 'the click is intercepted', prevented, true);
        check('cross-entity-fragment', 'the click opens the referenced entity',
            calls, [{ fn: 'openTerm', name: target, type: 'Setting' }]);
    }

    /// Scenario 2: a bare fragment whose id really is an anchor of the rendered document stays
    /// an in-page anchor - even when the id also names a documented entity, the document wins.
    {
        const a = rewriteOne(new FakeAnchor('#globs-in-path', 'globs in path'), ['globs-in-path']);
        check('in-page-anchor', 'the href is untouched', a.href, '#globs-in-path');
        check('in-page-anchor', 'the link is not an entity link', a.classList.contains('doc-internal-link'), false);
        a.click();
        check('in-page-anchor', 'the click navigates within the page', calls, [{ fn: 'openSection', id: 'globs-in-path' }]);

        const b = rewriteOne(new FakeAnchor('#file', 'file'), ['file']);
        b.click();
        check('in-page-anchor', 'an in-page anchor wins over a same-named entity', calls, [{ fn: 'openSection', id: 'file' }]);
    }

    /// Scenario 3: the "#" heading anchors of `processHeadingAnchors` are left alone. Their href
    /// is a whole app state hash; the fragment handling used to take the encoded state for an
    /// element id and push a corrupted `section=q%3D...` URL on top of the intended navigation.
    {
        const state_href = '#q=values&name=values&type=Table+Function&section=syntax';
        const a = rewriteOne(new FakeAnchor(state_href, '#', ['heading-anchor']));
        check('heading-anchor', 'the state href is untouched', a.href, state_href);
        check('heading-anchor', 'no extra click handler is attached', a.listeners.length, 0);
        check('heading-anchor', 'clicking performs no extra navigation', [a.click(), calls], [false, []]);
    }

    /// Scenario 4: an ambiguous fragment - a name documented under several types, with no route
    /// in the href to disambiguate - is not guessed: it stays an in-page anchor.
    {
        const ambiguous = [...all_names.keys()].find(k => all_names.get(k).length > 1 && /^[a-z_]\w*$/.test(k));
        check('ambiguous-fragment', 'the corpus has an ambiguous name', Boolean(ambiguous), true);
        const a = rewriteOne(new FakeAnchor('#' + ambiguous, ambiguous));
        check('ambiguous-fragment', 'the href is untouched', a.href, '#' + ambiguous);
        a.click();
        check('ambiguous-fragment', 'the click stays an in-page navigation', calls, [{ fn: 'openSection', id: ambiguous }]);
    }

    /// Scenario 5: a fragment with no in-page target that names no documented entity (a heading
    /// that exists only on the website's combined pages, e.g. `#comparison-rules`) stays an
    /// in-page anchor rather than resolving to a wrong entity.
    {
        const a = rewriteOne(new FakeAnchor('#comparison-rules', 'comparison rules'));
        check('unknown-fragment', 'the href is untouched', a.href, '#comparison-rules');
        a.click();
        check('unknown-fragment', 'the click stays an in-page navigation', calls, [{ fn: 'openSection', id: 'comparison-rules' }]);
    }

    /// Scenario 6: the pre-existing behaviors around the fragment branch still hold - a relative
    /// link to a documented entity opens it in the app (the route disambiguates the type), and an
    /// absolute link is left external, opening in a new tab.
    {
        const a = rewriteOne(new FakeAnchor('../table-functions/file.md', 'file'));
        a.click();
        check('relative-entity-link', 'the click opens the entity named by the route',
            calls, [{ fn: 'openTerm', name: 'file', type: 'Table Function' }]);

        const b = rewriteOne(new FakeAnchor('https://example.com/page', 'example'));
        check('absolute-link', 'the href is untouched', b.href, 'https://example.com/page');
        check('absolute-link', 'it opens in a new tab', [b.getAttribute('target'), b.getAttribute('rel')], ['_blank', 'noopener']);
        check('absolute-link', 'no click handler is attached', b.listeners.length, 0);
    }

    if (failures) {
        console.log(`${failures} check(s) failed`);
        process.exit(1);
    }
    console.log('All scenarios passed');
}

main().catch(e => { console.error(e); process.exit(1); });
