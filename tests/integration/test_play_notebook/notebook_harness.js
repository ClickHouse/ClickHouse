#!/usr/bin/env node
/// Executable regression harness for the `/play` NOTEBOOK mode: several query / text cells in one
/// tab, the shared run row docked under one of them, and the per-cell display state.
///
/// Runs the REAL script extracted from the served `play.html` inside a Node `vm` context with a
/// stubbed browser environment (DOM elements, `history`, `location`, `localStorage` and a small
/// functional in-memory IndexedDB fake), then drives the notebook the way the UI does and asserts
/// where the state lands. It covers the contracts a grep of the page cannot check:
///   * the shared Logs / Metrics toggles and the logo follow the cell the run row is DOCKED under
///     (`chromeCell`) - the RUNNING cell while a run is in flight - not the cell the editor moved to;
///   * color modes and pinned columns are per CELL: a toggle in one cell persists onto that cell's
///     own result snapshot (so it survives a reload) and does not rewrite another cell's state,
///     while the several result tables of one cell's "Run all" share one object;
///   * Markdown link/image targets: relative targets are allowed, schemes other than
///     `http`/`https`/`mailto` and protocol-relative `//host` are not.
///
/// Driven by `test.py` inside the `clickhouse/mysql-js-client` container (node:22-alpine),
/// against the `/play` page served by a real ClickHouse server. Can also be run standalone
/// against a checkout for development: node notebook_harness.js programs/server/play.html
///
/// Usage: node notebook_harness.js <path-or-url-of-play.html>
/// Exit code 0 = all scenarios pass; 1 = failure (details on stdout).
'use strict';

const vm = require('vm');
const fs = require('fs');

/// ----- Fake DOM -----------------------------------------------------------------

function makeStyle() {
    return new Proxy({
        setProperty() {},
        removeProperty() {},
        getPropertyValue() { return ''; },
    }, {
        get(target, prop) {
            if (prop in target) return target[prop];
            return '';
        },
        set(target, prop, value) { target[prop] = value; return true; },
    });
}

function makeClassList() {
    const set = new Set();
    return {
        add(...cs) { for (const c of cs) set.add(c); },
        remove(...cs) { for (const c of cs) set.delete(c); },
        toggle(c, force) {
            const on = force === undefined ? !set.has(c) : !!force;
            if (on) set.add(c); else set.delete(c);
            return on;
        },
        contains(c) { return set.has(c); },
    };
}

function makeElement(tag) {
    const listeners = new Map();
    const attributes = new Map();
    const el = {
        tagName: String(tag || 'div').toUpperCase(),
        nodeType: 1,
        id: '',
        style: makeStyle(),
        classList: makeClassList(),
        dataset: {},
        children: [],
        childNodes: [],
        parentNode: null,
        parentElement: null,
        firstChild: null,
        lastChild: null,
        nextSibling: null,
        previousSibling: null,
        value: '',
        textContent: '',
        innerHTML: '',
        innerText: '',
        title: '',
        placeholder: '',
        className: '',
        name: '',
        type: '',
        href: '',
        hidden: false,
        disabled: false,
        checked: false,
        readOnly: false,
        contentEditable: 'inherit',
        spellcheck: true,
        tabIndex: 0,
        selectionStart: 0,
        selectionEnd: 0,
        selectionDirection: 'none',
        scrollTop: 0,
        scrollLeft: 0,
        scrollHeight: 0,
        scrollWidth: 0,
        clientHeight: 0,
        clientWidth: 0,
        offsetHeight: 0,
        offsetWidth: 0,
        offsetTop: 0,
        offsetLeft: 0,

        addEventListener(type, fn) {
            if (!listeners.has(type)) listeners.set(type, []);
            listeners.get(type).push(fn);
        },
        removeEventListener(type, fn) {
            const l = listeners.get(type);
            if (l) {
                const i = l.indexOf(fn);
                if (i !== -1) l.splice(i, 1);
            }
        },
        dispatchEvent(ev) {
            try {
                Object.defineProperty(ev, 'target', { value: el, configurable: true });
                Object.defineProperty(ev, 'currentTarget', { value: el, configurable: true });
            } catch (e) { /* already defined */ }
            for (const fn of (listeners.get(ev.type) || []).slice()) fn.call(el, ev);
            const handler = el['on' + ev.type];
            if (typeof handler === 'function') handler.call(el, ev);
            return true;
        },
        appendChild(c) {
            el.children.push(c);
            el.childNodes.push(c);
            c.parentNode = el;
            c.parentElement = el;
            el.firstChild = el.children[0];
            el.lastChild = c;
            return c;
        },
        removeChild(c) {
            el.children = el.children.filter(x => x !== c);
            el.childNodes = el.childNodes.filter(x => x !== c);
            el.firstChild = el.children[0] || null;
            el.lastChild = el.children[el.children.length - 1] || null;
            return c;
        },
        insertBefore(c, ref) {
            const i = el.children.indexOf(ref);
            if (i === -1) return el.appendChild(c);
            el.children.splice(i, 0, c);
            el.childNodes.splice(i, 0, c);
            c.parentNode = el;
            c.parentElement = el;
            el.firstChild = el.children[0];
            return c;
        },
        replaceChildren(...cs) {
            el.children = [...cs];
            el.childNodes = [...cs];
            for (const c of cs) { c.parentNode = el; c.parentElement = el; }
            el.firstChild = el.children[0] || null;
            el.lastChild = el.children[el.children.length - 1] || null;
        },
        remove() { if (el.parentNode) el.parentNode.removeChild(el); },
        setAttribute(k, v) { attributes.set(k, String(v)); if (k === 'id') el.id = String(v); },
        getAttribute(k) { return attributes.has(k) ? attributes.get(k) : null; },
        removeAttribute(k) { attributes.delete(k); },
        hasAttribute(k) { return attributes.has(k); },
        focus() {},
        blur() {},
        click() { el.dispatchEvent(new Event('click')); },
        select() {},
        setSelectionRange(a, b) { el.selectionStart = a; el.selectionEnd = b; },
        getBoundingClientRect() { return { top: 0, left: 0, right: 0, bottom: 0, width: 0, height: 0, x: 0, y: 0 }; },
        getClientRects() { return []; },
        querySelector() { return null; },
        querySelectorAll() { return []; },
        closest() { return null; },
        matches() { return false; },
        contains() { return false; },
        scrollIntoView() {},
        scrollTo() {},
        scroll() {},
        cloneNode() { return makeElement(el.tagName); },
        insertAdjacentElement() {},
        insertAdjacentHTML() {},
        insertAdjacentText() {},
        getContext() { return null; },
        /// Methods of the <query-result> / <query-progress> custom elements: with a stub DOM the
        /// custom-element upgrade never happens, so provide inert versions of everything the
        /// script calls on them. The seeded run-backed snapshot carries no `data`, so
        /// `restoreFromHistory` bails out before any real rendering.
        clear() {},
        update() { return true; },
        updateRaw() {},
        renderError() {},
        clearError() {},
        clearSelection() {},
        flushFragment() {},
        async renderChart() {},
        redrawChart() {},
        renderGraph() {},
        renderTotals() {},
        applyColumnColors() {},
        applyPinnedColumns() {},
        refreshColumnColor() {},
        refreshSortIndicators() {},
        refreshFilterIndicators() {},
        refreshCellControls() {},
        renderPagination() {},
        transposeIfNeeded() {},
        expandSingleValueIfNeeded() {},
        _changeTableLayout() {},
        finalizeFailedTable() {},
        start() {},
        finish() {},
        clearBar() {},
        updateProgress() {},
        updateText() {},
        resetViewToggles() {},
        setViewState() {},
        showView() {},
        enableViews() {},
        finalizeMetrics() {},
        feedProfileEvents() {},
        adoptResourceState() {},
        renderResourcesFrom() {},
        appendLog() {},
        updateMetrics() {},
        attachShadow() { return makeElement('shadow-root'); },
    };
    return el;
}

function makeDocument() {
    const byId = new Map();
    const doc = makeElement('#document');
    doc.nodeType = 9;
    doc.readyState = 'complete';
    doc.visibilityState = 'visible';
    doc.hidden = false;
    doc.cookie = '';
    doc.body = makeElement('body');
    doc.head = makeElement('head');
    doc.documentElement = makeElement('html');
    doc.activeElement = doc.body;
    doc.getElementById = (id) => {
        if (!byId.has(id)) {
            const el = makeElement('div');
            el.id = id;
            byId.set(id, el);
        }
        return byId.get(id);
    };
    doc.createElement = (tag) => makeElement(tag);
    doc.createElementNS = (ns, tag) => makeElement(tag);
    doc.createTextNode = (text) => {
        const el = makeElement('#text');
        el.nodeType = 3;
        el.textContent = String(text);
        return el;
    };
    doc.createDocumentFragment = () => makeElement('#document-fragment');
    doc.createRange = () => ({
        selectNodeContents() {},
        setStart() {},
        setEnd() {},
        collapse() {},
        cloneRange() { return this; },
        getBoundingClientRect() { return { top: 0, left: 0, right: 0, bottom: 0, width: 0, height: 0 }; },
        getClientRects() { return []; },
    });
    doc.execCommand = () => false;
    doc.queryCommandSupported = () => false;
    const bySelector = new Map();
    doc.querySelector = (sel) => {
        if (!bySelector.has(sel)) bySelector.set(sel, makeElement('div'));
        return bySelector.get(sel);
    };
    doc.querySelectorAll = () => [];
    doc.hasFocus = () => true;
    /// The favicon <link> carries a base64 SVG data URL that the script recolors at load.
    doc.querySelector('link[rel="icon"]').href =
        'data:image/svg+xml;base64,' + Buffer.from('<svg fill="#ff0"></svg>').toString('base64');
    return doc;
}

/// ----- Fake IndexedDB (only what `openDb`/`loadFromDb`/`persist` use) --------------------

function makeIndexedDB(seedTabs, seedMeta, openDelayMs) {
    const stores = new Map();
    stores.set('tabs', { keyPath: 'id', data: new Map((seedTabs || []).map(r => [r.id, structuredClone(r)])) });
    stores.set('meta', { keyPath: 'key', data: new Map(seedMeta ? [['state', structuredClone(seedMeta)]] : []) });
    /// `openFired` records that the load window has closed: the open callback below has run.
    const stats = { persistCount: 0, openFired: false };

    function makeStoreHandle(name) {
        const s = stores.get(name);
        return {
            getAll() { return { result: [...s.data.values()].map(v => structuredClone(v)) }; },
            get(key) {
                const v = s.data.get(key);
                return { result: v === undefined ? undefined : structuredClone(v) };
            },
            put(obj) {
                s.data.set(obj[s.keyPath], structuredClone(obj));
                /// `persist` writes the meta `state` record last; count completed workspace saves.
                if (name === 'meta' && obj.key === 'state') stats.persistCount++;
                return { result: obj[s.keyPath] };
            },
            clear() { s.data.clear(); return { result: undefined }; },
            delete(key) { s.data.delete(key); return { result: undefined }; },
        };
    }

    const indexedDB = {
        open(name, version) {
            const req = { onupgradeneeded: null, onsuccess: null, onerror: null, result: null };
            /// `openDelayMs` lets a scenario make `IndexedDB.open` slower than any auto-run that
            /// races startup reconciliation (see the stale-reload-run-race scenario).
            setTimeout(() => {
                stats.openFired = true;
                req.result = {
                    objectStoreNames: { contains: (n) => stores.has(n) },
                    createObjectStore(n, opts) {
                        if (!stores.has(n)) stores.set(n, { keyPath: opts.keyPath, data: new Map() });
                        return makeStoreHandle(n);
                    },
                    transaction(names, mode) {
                        const tx = { oncomplete: null, onerror: null, onabort: null };
                        tx.objectStore = (n) => makeStoreHandle(n);
                        setTimeout(() => { if (tx.oncomplete) tx.oncomplete(); }, 0);
                        return tx;
                    },
                    close() {},
                };
                if (req.onsuccess) req.onsuccess();
            }, openDelayMs || 0);
            return req;
        },
    };
    return { indexedDB, stores, stats };
}

/// ----- Other browser globals ------------------------------------------------------------

function makeStorage() {
    const map = new Map();
    return {
        getItem(k) { return map.has(k) ? map.get(k) : null; },
        setItem(k, v) { map.set(String(k), String(v)); },
        removeItem(k) { map.delete(k); },
        clear() { map.clear(); },
        key(i) { return [...map.keys()][i] ?? null; },
        get length() { return map.size; },
    };
}

function makeLocation(href) {
    const u = new URL(href);
    return {
        get href() { return u.href; },
        get origin() { return u.origin; },
        get protocol() { return u.protocol; },
        get host() { return u.host; },
        get hostname() { return u.hostname; },
        get port() { return u.port; },
        get pathname() { return u.pathname; },
        get search() { return u.search; },
        get hash() { return u.hash; },
        set hash(h) { u.hash = h; },
        toString() { return u.href; },
        assign() {},
        replace() {},
        reload() {},
        _apply(url) {
            const next = new URL(url, u.href);
            u.href = next.href;
        },
    };
}

function makeHistory(initialState, location) {
    return {
        state: initialState,
        length: 1,
        replaceState(state, title, url) {
            this.state = state;
            if (url !== undefined && url !== null) location._apply(String(url));
        },
        pushState(state, title, url) {
            this.state = state;
            this.length++;
            if (url !== undefined && url !== null) location._apply(String(url));
        },
        back() {},
        forward() {},
        go() {},
    };
}

/// ----- Context assembly -------------------------------------------------------------------

function makeContext({ href, historyState, seedTabs, seedMeta, openDelayMs, wasmInstantiateDelayMs, disableWasm }) {
    const document = makeDocument();
    const location = makeLocation(href);
    const history = makeHistory(historyState, location);
    const { indexedDB, stores, stats } = makeIndexedDB(seedTabs, seedMeta, openDelayMs);

    const sandbox = {
        document,
        location,
        history,
        indexedDB,
        localStorage: makeStorage(),
        sessionStorage: makeStorage(),
        navigator: {
            clipboard: { writeText: async () => {}, readText: async () => '' },
            platform: 'Linux x86_64',
            language: 'en-US',
            userAgent: 'play-notebook-harness',
        },
        /// Deterministic environment: no network. The only top-level fetch (the webterminal
        /// probe) checks `resp.ok`, and every other call site handles a non-ok response.
        fetch: async () => ({
            ok: false,
            status: 503,
            statusText: 'harness: network disabled',
            headers: { get: () => null },
            text: async () => '',
            json: async () => ({}),
        }),
        setTimeout, clearTimeout, setInterval, clearInterval,
        queueMicrotask,
        requestAnimationFrame: (fn) => setTimeout(fn, 0),
        cancelAnimationFrame: (t) => clearTimeout(t),
        requestIdleCallback: (fn) => setTimeout(fn, 0),
        cancelIdleCallback: (t) => clearTimeout(t),
        console,
        performance: { now: () => Date.now() },
        crypto: require('node:crypto').webcrypto,
        atob: (b64) => Buffer.from(b64, 'base64').toString('binary'),
        btoa: (bin) => Buffer.from(bin, 'binary').toString('base64'),
        TextEncoder, TextDecoder,
        URL, URLSearchParams,
        Event, CustomEvent,
        AbortController,
        structuredClone,
        HTMLElement: class HTMLElement {},
        customElements: { define() {}, get() { return undefined; }, whenDefined() { return Promise.resolve(); } },
        ResizeObserver: class ResizeObserver { observe() {} unobserve() {} disconnect() {} },
        MutationObserver: class MutationObserver { observe() {} disconnect() {} takeRecords() { return []; } },
        IntersectionObserver: class IntersectionObserver { observe() {} unobserve() {} disconnect() {} },
        matchMedia: () => ({ matches: false, media: '', addEventListener() {}, removeEventListener() {}, addListener() {}, removeListener() {} }),
        getComputedStyle: () => new Proxy({ getPropertyValue: () => '' }, { get(t, p) { return p in t ? t[p] : ''; } }),
        getSelection: () => ({ removeAllRanges() {}, addRange() {}, toString() { return ''; }, rangeCount: 0 }),
        alert() {}, confirm() { return false; }, prompt() { return null; },
        scrollTo() {}, scroll() {},
        innerHeight: 800, innerWidth: 1280, devicePixelRatio: 1,
        addEventListener() {}, removeEventListener() {},
        /// `wasmInstantiateDelayMs` keeps the WASM lexer "still loading" past startup settlement
        /// (`wasmAvailable()` only checks the shape, so the wrapper still counts as available):
        /// the window where a history write can race the first `WebAssembly.instantiate`.
        /// `disableWasm` removes `WebAssembly` outright (`wasmAvailable()` is false), so the
        /// lexer NEVER becomes available — the window where even the debounced persist and a
        /// later reload cannot rebuild the parameter snapshot.
        WebAssembly: disableWasm
            ? undefined
            : wasmInstantiateDelayMs
                ? { instantiate: (...args) => new Promise((resolve, reject) =>
                      setTimeout(() => WebAssembly.instantiate(...args).then(resolve, reject),
                                 wasmInstantiateDelayMs)) }
                : WebAssembly,
    };
    sandbox.window = sandbox;
    sandbox.self = sandbox;
    sandbox.globalThis = sandbox;
    vm.createContext(sandbox);
    return { sandbox, stores, stats };
}

/// ----- Scenario driver ----------------------------------------------------------------------

function extractScript(html) {
    const blocks = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)].map(m => m[1]);
    if (!blocks.length) throw new Error('no <script> block found in play.html');
    return blocks.reduce((a, b) => (a.length >= b.length ? a : b));
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function runScenario(js, config) {
    const { sandbox, stores, stats } = makeContext(config);
    vm.runInContext(js, sandbox, { filename: 'play.html.js' });
    /// A scenario may interact with the bootstrap workspace while IndexedDB is still opening
    /// (see the dirty-startup scenario): run `config.duringLoad(sandbox)` inside the `openDelayMs`
    /// window, before `reconcileStartup` takes over the workspace (`bootstrap_settled`).
    if (config.duringLoad) {
        /// The bootstrap is synchronous and `IndexedDB.open` can only resolve through a
        /// `setTimeout`, so yielding to microtasks alone keeps the interaction before the open.
        await Promise.resolve();
        if (stats.openFired)
            throw new Error('duringLoad ran after the IndexedDB open completed: the load window closed early');
        if (vm.runInContext('bootstrap_settled', sandbox))
            throw new Error('duringLoad ran after reconciliation settled: the load window closed early');
        config.duringLoad(sandbox);
        if (!vm.runInContext('bootstrap_dirty', sandbox))
            throw new Error('duringLoad did not mark the bootstrap workspace dirty');
    }
    /// Startup is asynchronous: `reconcileStartup` awaits IndexedDB and ends with the debounced
    /// `scheduleSave` (400 ms), whose `persist` writes the reconciled workspace back. Wait for
    /// that write — it marks reconciliation as complete and persisted.
    const deadline = Date.now() + 15000;
    while (stats.persistCount < 1) {
        if (Date.now() > deadline) throw new Error('timed out waiting for the startup persist');
        await sleep(25);
    }
    await sleep(50);
    const live = vm.runInContext(
        'JSON.stringify({ tabs: tabs.map(t => ({ id: t.id, title: t.title, query: t.query, ran: !!(t.result && t.result.ran) })), activeTabId })',
        sandbox);
    const persisted = [...stores.get('tabs').data.values()];
    const persistedMeta = stores.get('meta').data.get('state') || null;
    return { live: JSON.parse(live), persisted, persistedMeta, sandbox, stores, stats };
}

/// Wait for the next debounced `persist` (400 ms) after a post-startup interaction, then read back
/// what it wrote. Used by the scenarios that assert what a reload would find in `IndexedDB`.
async function waitForNextPersist(r) {
    const target = r.stats.persistCount + 1;
    const deadline = Date.now() + 5000;
    while (r.stats.persistCount < target) {
        if (Date.now() > deadline) throw new Error('timed out waiting for the debounced persist');
        await sleep(25);
    }
    await sleep(25);
    return [...r.stores.get('tabs').data.values()];
}

/// ----- Assertions ----------------------------------------------------------------------------

let failures = 0;

function check(scenario, what, cond, actual) {
    if (cond) {
        console.log(`PASS [${scenario}] ${what}`);
    } else {
        failures++;
        console.log(`FAIL [${scenario}] ${what} -- actual: ${JSON.stringify(actual)}`);
    }
}

/// Evaluate an expression inside the page's realm and bring the result back as plain data.
function evalJSON(sandbox, expr) {
    return JSON.parse(vm.runInContext(`JSON.stringify((() => { ${expr} })())`, sandbox));
}

async function main() {
    const src = process.argv[2];
    if (!src) {
        console.error('usage: node notebook_harness.js <path-or-url-of-play.html>');
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
    const base = 'http://127.0.0.1:8123/play';

    /// Contract 1: the shared run row - its Logs / Metrics toggles and the logo - describes the
    /// cell it is DOCKED under. While a run is in flight that is the RUNNING cell, so moving the
    /// editor to another cell mid-run must not retarget the toggles at the newly active cell nor
    /// regrow the idle logo over a running (multi-query) row. Once the run is over the row goes
    /// back to following the active cell.
    {
        const scenario = 'chrome-follows-running-cell';
        const r = await runScenario(js, { href: base });
        const out = evalJSON(r.sandbox, `
            const tab = getActiveTab();
            /// Two query cells; \`addCell\` hands the editor to the new one.
            addCell(tab, 'query', tab.cells.length);
            const [a, b] = tab.cells;
            tab.activeCellId = b.id;
            a.view = 'result'; b.view = 'result';
            /// A "Run all" in cell A hides the logo for A only (see \`postMulti\`).
            a.logoVisible = false; b.logoVisible = true;
            tab.inFlight = true; tab.runCell = a;
            progressEl.dispatchEvent(new CustomEvent('set-view', { detail: { view: 'logs' } }));
            syncActiveTabChrome();
            const running = { aView: a.view, bView: b.view, logo: logoEl.style.display };
            tab.inFlight = false; tab.runCell = null;
            progressEl.dispatchEvent(new CustomEvent('set-view', { detail: { view: 'metrics' } }));
            syncActiveTabChrome();
            const idle = { aView: a.view, bView: b.view, logo: logoEl.style.display };
            return { running, idle, cells: tab.cells.length };
        `);
        check(scenario, 'the notebook has two cells', out.cells === 2, out.cells);
        check(scenario, 'a mid-run view toggle acts on the running cell',
              out.running.aView === 'logs', out.running);
        check(scenario, 'the cell the editor moved to is not retargeted',
              out.running.bView === 'result', out.running);
        check(scenario, 'the logo stays hidden for the running cell',
              out.running.logo === 'none', out.running);
        check(scenario, 'after the run the toggles follow the active cell again',
              out.idle.bView === 'metrics' && out.idle.aView === 'logs', out.idle);
        check(scenario, 'after the run the logo follows the active cell again',
              out.idle.logo === 'block', out.idle);
    }

    /// Contract 2: color modes and pinned columns belong to ONE cell. A toggle made in a cell that
    /// is not the active one records onto THAT cell's result snapshot (which is what a reload
    /// reads back) and leaves the other cells alone; the URL and the history entry, which describe
    /// the active cell, are only re-stamped for a toggle made in the active cell.
    {
        const scenario = 'color-state-is-per-cell';
        const r = await runScenario(js, { href: base });
        const out = evalJSON(r.sandbox, `
            const tab = getActiveTab();
            addCell(tab, 'query', tab.cells.length);
            const [a, b] = tab.cells;
            tab.activeCellId = b.id;
            /// Both cells hold a result whose snapshot carried no explicit color state.
            a.result = { ok: true, data: null };
            b.result = { ok: true, data: null };

            /// A toggle in the NON-active cell A.
            a.colorModes['id'] = 'heatmap';
            a.pinnedColumns['id'] = true;
            persistColorModes(a);
            persistPinnedColumns(a);
            const after_a = {
                a_snapshot: a.result.color_modes,
                a_pins: a.result.pinned_columns,
                b_modes: { ...b.colorModes },
                b_snapshot_modes: b.result.color_modes ?? null,
                url: location.href,
                serialized: serializeCell(a).result.color_modes,
                shared_with_element: a.resultEl ? a.resultEl._colorModes === a.colorModes : null,
                distinct_objects: a.colorModes !== b.colorModes,
            };

            /// A toggle in the ACTIVE cell B additionally re-stamps the URL and history entry.
            b.colorModes['value'] = 'bar';
            persistColorModes(b);
            const after_b = {
                url_modes: new URL(location.href).searchParams.get('color_modes'),
                b_snapshot: b.result.color_modes,
                a_untouched: { ...a.colorModes },
            };
            return { after_a, after_b };
        `);
        const a = out.after_a;
        check(scenario, "a non-active cell's toggle lands on its own snapshot",
              a.a_snapshot && a.a_snapshot.id === 'heatmap', a.a_snapshot);
        check(scenario, "a non-active cell's pin lands on its own snapshot",
              a.a_pins && a.a_pins.id === true, a.a_pins);
        check(scenario, 'it survives the persistence round-trip',
              a.serialized && a.serialized.id === 'heatmap', a.serialized);
        check(scenario, 'the other cell keeps its own (empty) state',
              Object.keys(a.b_modes).length === 0 && a.b_snapshot_modes === null, a);
        check(scenario, 'the cells hold distinct state objects', a.distinct_objects === true, a);
        check(scenario, "the cell's result element shares the cell's object",
              a.shared_with_element === true, a);
        check(scenario, 'the URL is not re-stamped from a non-active cell',
              !/color_modes|pinned_columns/.test(a.url), a.url);
        const b = out.after_b;
        check(scenario, "the active cell's toggle re-stamps the URL",
              b.url_modes === JSON.stringify({ value: 'bar' }), b.url_modes);
        check(scenario, "the active cell's toggle lands on its own snapshot",
              b.b_snapshot && b.b_snapshot.value === 'bar', b.b_snapshot);
        check(scenario, "it does not rewrite the other cell's state",
              b.a_untouched && b.a_untouched.id === 'heatmap' && !b.a_untouched.value, b.a_untouched);
    }

    /// Contract 3: a text cell's Markdown may link to ordinary relative targets of the page, while
    /// anything naming a scheme other than `http`/`https`/`mailto` - and a protocol-relative
    /// `//host`, which is another origin rather than a relative target - is left unrendered.
    {
        const scenario = 'markdown-relative-links';
        const r = await runScenario(js, { href: base });
        const out = evalJSON(r.sandbox, `
            const allowed = ['guide.md', 'images/pic.png', '?q=1', '#section', '/absolute',
                             './rel', '../up', 'a/b/c.md?x=1#f', 'https://clickhouse.com/',
                             'mailto:x@example.com'];
            const rejected = ['javascript:alert(1)', 'JavaScript:alert(1)', 'data:text/html,<b>',
                              'vbscript:msgbox', '//evil.example.com/x', '\\u0000javascript:alert(1)'];
            return {
                allowed: allowed.filter(u => safeMarkdownUrl(u) !== u),
                rejected: rejected.filter(u => safeMarkdownUrl(u) !== ''),
                link: renderMarkdown('[guide](guide.md)'),
                image: renderMarkdown('![pic](images/pic.png)'),
                query: renderMarkdown('[search](?q=1)'),
                script: renderMarkdown('[x](javascript:alert(1))'),
                protocol_relative: renderMarkdown('[x](//evil.example.com/x)'),
            };
        `);
        check(scenario, 'relative targets are allowed', out.allowed.length === 0, out.allowed);
        check(scenario, 'other schemes are rejected', out.rejected.length === 0, out.rejected);
        check(scenario, 'a relative link renders', out.link.includes('href="guide.md"'), out.link);
        check(scenario, 'a relative image renders', out.image.includes('src="images/pic.png"'), out.image);
        check(scenario, 'a query-only link renders', out.query.includes('href="?q=1"'), out.query);
        check(scenario, 'a javascript: link does not render',
              !out.script.includes('<a href'), out.script);
        check(scenario, 'a protocol-relative link does not render',
              !out.protocol_relative.includes('<a href'), out.protocol_relative);
    }

    /// Contract 3a: block structure of the Markdown renderer. A block quote ends at the first
    /// line that does not itself start with '>', and a fenced code block only closes on a fence
    /// of the same character that is at least as long as the opener - so a longer fence can show
    /// a shorter one inside a code block.
    {
        const scenario = 'markdown-block-boundaries';
        const r = await runScenario(js, { href: base });
        const out = evalJSON(r.sandbox, `
            return {
                quote_then_text: renderMarkdown('> quoted\\nnot quoted'),
                quote_two_lines: renderMarkdown('> first\\n> second\\nafter'),
                long_fence: renderMarkdown('\\u0060\\u0060\\u0060\\u0060\\n\\u0060\\u0060\\u0060\\n\\u0060\\u0060\\u0060\\u0060'),
                tilde_fence: renderMarkdown('~~~~\\n~~~\\n~~~~'),
                fence_trailing_text: renderMarkdown('\\u0060\\u0060\\u0060\\ncode\\n\\u0060\\u0060\\u0060'),
            };
        `);
        check(scenario, 'a quote ends at the first non-quoted line',
              out.quote_then_text.includes('quoted</p></blockquote>')
              && !out.quote_then_text.match(/<blockquote>[\s\S]*not quoted[\s\S]*<\/blockquote>/),
              out.quote_then_text);
        check(scenario, 'the non-quoted line becomes its own paragraph',
              out.quote_then_text.includes('<p>not quoted</p>'), out.quote_then_text);
        check(scenario, 'consecutive quoted lines stay one quote',
              (out.quote_two_lines.match(/<blockquote>/g) || []).length === 1
              && out.quote_two_lines.match(/<blockquote>[\s\S]*first[\s\S]*second[\s\S]*<\/blockquote>/)
              && !out.quote_two_lines.match(/<blockquote>[\s\S]*after[\s\S]*<\/blockquote>/),
              out.quote_two_lines);
        check(scenario, 'a longer backtick fence shows a shorter one inside',
              (out.long_fence.match(/<pre>/g) || []).length === 1
              && out.long_fence.includes('<pre><code>\`\`\`</code></pre>'),
              out.long_fence);
        check(scenario, 'a longer tilde fence shows a shorter one inside',
              (out.tilde_fence.match(/<pre>/g) || []).length === 1
              && out.tilde_fence.includes('<pre><code>~~~</code></pre>'),
              out.tilde_fence);
        check(scenario, 'an equal-length fence still closes the block',
              out.fence_trailing_text.includes('<pre><code>code</code></pre>'),
              out.fence_trailing_text);
    }

    /// Contract 4: stopping a run AFTER the editor moved to another cell repaints the shared row
    /// from the newly active cell right away. `cancelTabRun` goes through `abortTabQuery`, which
    /// forgets `tab.runCell` before `endFlight` decides how to repaint - the stopped cell must be
    /// handed through explicitly, or the row stays painted with the stopped cell's cleared state
    /// under the active cell until some later activation.
    {
        const scenario = 'stop-after-editor-moved-repaints-chrome';
        const r = await runScenario(js, { href: base });
        const out = evalJSON(r.sandbox, `
            const tab = getActiveTab();
            addCell(tab, 'query', tab.cells.length);
            const [a, b] = tab.cells;
            tab.activeCellId = b.id;
            a.view = 'result'; b.view = 'result';
            /// Cell A's (multi-query) run hid the logo for A; B has an idle result showing it.
            a.logoVisible = false; b.logoVisible = true;
            tab.inFlight = true; tab.runCell = a;
            a.progressPhase = 'running';
            syncActiveTabChrome();
            const running = { logo: logoEl.style.display };
            /// The Run->Stop button, pressed while the editor is on B.
            cancelTabRun(tab);
            const stopped = {
                logo: logoEl.style.display,
                inFlight: tab.inFlight,
                runCell: tab.runCell,
                aPhase: a.progressPhase,
            };
            /// A view toggle right after the stop must act on B, not on the stopped A.
            progressEl.dispatchEvent(new CustomEvent('set-view', { detail: { view: 'logs' } }));
            return { running, stopped, aView: a.view, bView: b.view };
        `);
        check(scenario, 'while running the chrome follows the running cell',
              out.running.logo === 'none', out.running);
        check(scenario, 'the stop ends the flight',
              out.stopped.inFlight === false && out.stopped.runCell === null, out.stopped);
        check(scenario, "the stopped cell's progress state is reset",
              out.stopped.aPhase === 'idle', out.stopped);
        check(scenario, 'the shared row is repainted from the active cell immediately',
              out.stopped.logo === 'block', out.stopped);
        check(scenario, 'a toggle right after the stop acts on the active cell',
              out.bView === 'logs' && out.aView === 'result', { aView: out.aView, bView: out.bView });
    }

    /// Contract 5: a toggle in a non-active cell also refreshes that cell's serialized copy inside
    /// the CURRENT history entry. The entry's notebook (\`state.cells\`) is what Close tab + Back
    /// rebuilds the tab from, and \`closeTab\` does not refresh a background tab's entries - so
    /// without the in-place patch the toggle would be silently lost on that restore.
    {
        const scenario = 'history-entry-keeps-off-active-cell-state';
        const r = await runScenario(js, { href: base });
        const out = evalJSON(r.sandbox, `
            const tab = getActiveTab();
            addCell(tab, 'query', tab.cells.length);
            const [a, b] = tab.cells;
            tab.activeCellId = b.id;
            a.query = 'SELECT 1'; a.result = { ok: true, data: null };
            b.result = { ok: true, data: null };
            /// Write the entry the browser would hold before the user leaves via Back.
            writeHistoryEntry(tab);
            /// A color toggle and a pin in the VISIBLE, non-active cell A.
            a.colorModes['id'] = 'heatmap';
            persistColorModes(a);
            a.pinnedColumns['id'] = true;
            persistPinnedColumns(a);
            const entry = history.state;
            const a_copy = (entry.cells || []).find(c => c.id === a.id) || null;
            const b_copy = (entry.cells || []).find(c => c.id === b.id) || null;
            return {
                cells: (entry.cells || []).length,
                a_modes: (a_copy && a_copy.result && a_copy.result.color_modes) || null,
                a_pins: (a_copy && a_copy.result && a_copy.result.pinned_columns) || null,
                b_modes: (b_copy && b_copy.result && b_copy.result.color_modes) || null,
                url: location.href,
            };
        `);
        check(scenario, 'the entry carries the notebook', out.cells === 2, out.cells);
        check(scenario, "the entry's copy of the toggled cell carries the color mode",
              out.a_modes && out.a_modes.id === 'heatmap', out.a_modes);
        check(scenario, "the entry's copy of the toggled cell carries the pin",
              out.a_pins && out.a_pins.id === true, out.a_pins);
        check(scenario, "the other cell's copy is untouched",
              !out.b_modes || Object.keys(out.b_modes).length === 0, out.b_modes);
        check(scenario, 'the URL is still not re-stamped from a non-active cell',
              !/color_modes|pinned_columns/.test(out.url), out.url);
    }

    /// Contract 6: the history entry's notebook payload is bounded. Each cell's result snapshot is
    /// individually capped, so a LONG notebook would otherwise grow \`history.state\` linearly with
    /// the cell count until the browser rejects the write. Over the budget the largest snapshots
    /// are stripped (those cells restore query-only from the entry, like an oversized single
    /// result already does), and a write the browser still rejects degrades in place instead of
    /// throwing out of the run or tab operation that triggered it.
    {
        const scenario = 'history-payload-is-bounded';
        const r = await runScenario(js, { href: base });
        const out = evalJSON(r.sandbox, `
            const tab = getActiveTab();
            /// A notebook whose per-cell snapshots are each under the per-result cap while their
            /// sum is far over the notebook-wide budget.
            const chunk = 'x'.repeat(90000);
            for (let i = 0; i < 40; ++i) addCell(tab, 'query', tab.cells.length);
            for (const c of tab.cells) { c.query = 'SELECT 1'; c.result = { ok: true, data: chunk }; }
            writeHistoryEntry(tab);
            const entry = history.state;
            const kept = entry.cells.filter(c => c.result).length;
            const size = JSON.stringify(entry.cells).length;
            /// The live notebook (and through it IndexedDB) keeps every snapshot; only the entry
            /// is trimmed.
            const live = tab.cells.filter(c => c.result).length;

            /// A browser rejecting even the bounded entry: the write must degrade, not throw.
            const real_replace = history.replaceState.bind(history);
            let rejected = 0;
            history.replaceState = (st, title, url) => {
                if (st && st.cells && st.cells.some(c => c.result)) { ++rejected; throw new Error('quota'); }
                real_replace(st, title, url);
            };
            let threw = false;
            try { writeHistoryEntry(tab, null, true); } catch (e) { threw = true; }
            history.replaceState = real_replace;
            const degraded = history.state;
            return { cells: entry.cells.length, kept, size, live, threw, rejected,
                     degraded_results: degraded.cells.filter(c => c.result).length,
                     degraded_cells: degraded.cells.length,
                     degraded_query: degraded.cells[0] && degraded.cells[0].query };
        `);
        check(scenario, 'the entry still carries every cell', out.cells === 41, out.cells);
        check(scenario, 'the payload is bounded by the notebook budget', out.size <= 2100000, out.size);
        check(scenario, 'the snapshots that fit are kept', out.kept > 0 && out.kept < 41, out.kept);
        check(scenario, 'the trim strips snapshots only from the entry', out.live === 41, out.live);
        check(scenario, 'a rejected write degrades instead of throwing',
              out.threw === false && out.rejected > 0, out);
        check(scenario, 'the degraded entry keeps the notebook structure query-only',
              out.degraded_results === 0 && out.degraded_cells === 41 && out.degraded_query === 'SELECT 1',
              out);
    }

    if (failures) {
        console.log(`${failures} check(s) FAILED`);
        process.exit(1);
    }
    console.log('All scenarios passed');
}

main().catch(e => { console.error(e && e.stack || String(e)); process.exit(1); });
