#!/usr/bin/env node
/// Executable regression harness for the `/play` startup reconciliation (`reconcileStartup`).
///
/// Runs the REAL script extracted from the served `play.html` inside a Node `vm` context with a
/// stubbed browser environment (DOM elements, `history`, `location`, `localStorage` and a small
/// functional in-memory IndexedDB fake). Each scenario seeds the fake IndexedDB with saved tabs,
/// executes the page script (which calls `reconcileStartup` at top level), waits for the trailing
/// debounced `persist` to write the reconciled workspace back, and then asserts both the live
/// `tabs` state and what was persisted.
///
/// Driven by `test.py` inside the `clickhouse/mysql-js-client` container (node:22-alpine),
/// against the `/play` page served by a real ClickHouse server. Can also be run standalone
/// against a checkout for development: node reconcile_harness.js programs/server/play.html
///
/// Usage: node reconcile_harness.js <path-or-url-of-play.html>
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
        refreshColumnColor() {},
        transposeIfNeeded() {},
        _changeTableLayout() {},
        start() {},
        finish() {},
        updateProgress() {},
        updateText() {},
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
    const stats = { persistCount: 0 };

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

function makeContext({ href, historyState, seedTabs, seedMeta, openDelayMs }) {
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
            userAgent: 'play-reconcile-harness',
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
        WebAssembly,
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
        await sleep(config.duringLoadDelayMs || 5);
        config.duringLoad(sandbox);
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
    return { live: JSON.parse(live), persisted, persistedMeta, sandbox };
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

async function main() {
    const src = process.argv[2];
    if (!src) {
        console.error('usage: node reconcile_harness.js <path-or-url-of-play.html>');
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

    /// Contract 1: a mixed workspace (blank + non-blank saved tabs) restores only the
    /// non-blank tabs on a plain load; the blank one is pruned from IndexedDB too.
    {
        const r = await runScenario(js, {
            href: base,
            historyState: null,
            seedTabs: [
                { id: 't7', title: 'Scratch', query: '   \n  ', params: {}, result: null, lastSavedQuery: '' },
                { id: 't8', title: 'Report', query: 'SELECT 1', params: {}, result: null, lastSavedQuery: 'SELECT 1' },
            ],
            seedMeta: { key: 'state', activeTabId: 't8', tabOrder: ['t7', 't8'], tabSeq: 8, tabTitleSeq: 2 },
        });
        check('mixed', 'only the non-blank tab is restored',
            r.live.tabs.length === 1 && r.live.tabs[0].title === 'Report' && r.live.tabs[0].query === 'SELECT 1',
            r.live);
        check('mixed', 'the blank tab is pruned from IndexedDB',
            r.persisted.length === 1 && r.persisted[0].id === 't8',
            r.persisted.map(p => p.id));
    }

    /// Contract 2: an all-blank workspace falls back to a single fresh tab,
    /// exactly as on a first-ever visit.
    {
        const r = await runScenario(js, {
            href: base,
            historyState: null,
            seedTabs: [
                { id: 't7', title: 'Scratch', query: '', params: {}, result: null, lastSavedQuery: '' },
                { id: 't8', title: 'Notes', query: ' \t ', params: {}, result: null, lastSavedQuery: '' },
            ],
            seedMeta: { key: 'state', activeTabId: 't7', tabOrder: ['t7', 't8'], tabSeq: 8, tabTitleSeq: 2 },
        });
        check('all-blank', 'exactly one fresh empty tab remains',
            r.live.tabs.length === 1 && r.live.tabs[0].query.trim() === '',
            r.live);
        check('all-blank', 'neither blank record survives in IndexedDB',
            !r.persisted.some(p => p.title === 'Scratch' || p.title === 'Notes'),
            r.persisted.map(p => p.title));
    }

    /// Contract 3: a tab whose editor was cleared after a run still holds a `result.ran`
    /// snapshot and must be preserved, not pruned.
    {
        const r = await runScenario(js, {
            href: base,
            historyState: null,
            seedTabs: [
                { id: 't7', title: 'Ran', query: '', params: {}, result: { ran: true, query: 'SELECT 2', params: {} }, lastSavedQuery: 'SELECT 2' },
                { id: 't8', title: 'Scratch', query: '', params: {}, result: null, lastSavedQuery: '' },
            ],
            seedMeta: { key: 'state', activeTabId: 't7', tabOrder: ['t7', 't8'], tabSeq: 8, tabTitleSeq: 2 },
        });
        check('run-backed', 'the cleared-after-run tab survives, the blank one is pruned',
            r.live.tabs.length === 1 && r.live.tabs[0].title === 'Ran' && r.live.tabs[0].ran,
            r.live);
        check('run-backed', 'the run-backed record stays in IndexedDB',
            r.persisted.some(p => p.id === 't7' && p.result && p.result.ran) && !r.persisted.some(p => p.id === 't8'),
            r.persisted.map(p => p.id));
    }

    /// Guard: a plain reload whose URL still carries `?tab=<pruned blank tab>` (a stale echo:
    /// `history.state` was preserved by the reload and names the pruned tab) must NOT resurrect
    /// the blank tab; it falls back to a surviving saved tab.
    {
        const r = await runScenario(js, {
            href: base + '?tab=Scratch',
            historyState: { tabId: 't7', tabName: 'Scratch' },
            seedTabs: [
                { id: 't7', title: 'Scratch', query: '', params: {}, result: null, lastSavedQuery: '' },
                { id: 't8', title: 'Report', query: 'SELECT 1', params: {}, result: null, lastSavedQuery: 'SELECT 1' },
            ],
            seedMeta: { key: 'state', activeTabId: 't7', tabOrder: ['t7', 't8'], tabSeq: 8, tabTitleSeq: 2 },
        });
        check('stale-reload', 'the pruned blank tab is not resurrected; the survivor is restored',
            r.live.tabs.length === 1 && r.live.tabs[0].title === 'Report',
            r.live);
    }

    /// Guard (fresh external collision): a genuine external bare `?tab=Scratch` link — no
    /// `history.state` at all, so this is a fresh navigation, not a reload — must stay
    /// authoritative even though a locally pruned blank tab happened to share that title.
    /// `stale_blank_reload` is keyed on `history.state`, so `history.state === null` makes it
    /// false regardless of the title collision, and a new `Scratch` tab is opened rather than
    /// being conflated with the surviving `Report` tab.
    {
        const r = await runScenario(js, {
            href: base + '?tab=Scratch',
            historyState: null,
            seedTabs: [
                { id: 't7', title: 'Scratch', query: '', params: {}, result: null, lastSavedQuery: '' },
                { id: 't8', title: 'Report', query: 'SELECT 1', params: {}, result: null, lastSavedQuery: 'SELECT 1' },
            ],
            seedMeta: { key: 'state', activeTabId: 't7', tabOrder: ['t7', 't8'], tabSeq: 8, tabTitleSeq: 2 },
        });
        check('fresh-external-collision', 'a new Scratch tab is opened alongside the surviving Report',
            r.live.tabs.length === 2 && r.live.tabs.some(t => t.title === 'Report')
                && r.live.tabs.filter(t => t.title === 'Scratch').length === 1,
            r.live);
        check('fresh-external-collision', 'the new Scratch tab is the active one',
            r.live.tabs.find(t => t.title === 'Scratch').id === r.live.activeTabId,
            r.live);
    }

    /// Guard (stale reload, whitespace hash): a stale reload of a pruned blank tab can still carry
    /// that tab's own blank text in the hash (`?tab=<pruned blank>#<base64 of whitespace>`).
    /// Whitespace is not a real query by the same `trim()` rule the pruning uses, so
    /// `stale_blank_reload_bare` must treat it exactly like a bare `?tab=`: fall back to the
    /// surviving tab rather than recreating the pruned tab with its stray whitespace text.
    {
        const whitespace_hash = Buffer.from('   \n ', 'utf8').toString('base64');
        const r = await runScenario(js, {
            href: base + '?tab=Scratch#' + whitespace_hash,
            historyState: { tabId: 't7', tabName: 'Scratch' },
            seedTabs: [
                { id: 't7', title: 'Scratch', query: '', params: {}, result: null, lastSavedQuery: '' },
                { id: 't8', title: 'Report', query: 'SELECT 1', params: {}, result: null, lastSavedQuery: 'SELECT 1' },
            ],
            seedMeta: { key: 'state', activeTabId: 't7', tabOrder: ['t7', 't8'], tabSeq: 8, tabTitleSeq: 2 },
        });
        check('stale-reload-whitespace-hash', 'the pruned blank tab is not resurrected with its stray whitespace text',
            r.live.tabs.length === 1 && r.live.tabs[0].title === 'Report',
            r.live);
        check('stale-reload-whitespace-hash', 'no blank Scratch is re-persisted to IndexedDB',
            !r.persisted.some(p => p.title === 'Scratch'),
            r.persisted.map(p => p.title));
    }

    /// Guard (legacy history entry): the same stale reload, but with a `history.state` written by
    /// a pre-`tabId` version of the page — it carries only `tabName` (`resolveTabForState` still
    /// supports that shape via its title fallback). `stale_blank_reload` must recognize the pruned
    /// blank tab by title for such an entry; keying only on `history.state.tabId` would leave the
    /// guard false and let the authoritative path recreate and re-persist the blank tab on the
    /// first reload after an upgrade.
    {
        const r = await runScenario(js, {
            href: base + '?tab=Scratch',
            historyState: { tabName: 'Scratch', query: '', params: {}, result: null },
            seedTabs: [
                { id: 't7', title: 'Scratch', query: '', params: {}, result: null, lastSavedQuery: '' },
                { id: 't8', title: 'Report', query: 'SELECT 1', params: {}, result: null, lastSavedQuery: 'SELECT 1' },
            ],
            seedMeta: { key: 'state', activeTabId: 't7', tabOrder: ['t7', 't8'], tabSeq: 8, tabTitleSeq: 2 },
        });
        check('stale-reload-legacy', 'the pruned blank tab is not resurrected for a legacy pre-tabId entry',
            r.live.tabs.length === 1 && r.live.tabs[0].title === 'Report',
            r.live);
        check('stale-reload-legacy', 'no blank Scratch is re-persisted to IndexedDB',
            !r.persisted.some(p => p.title === 'Scratch'),
            r.persisted.map(p => p.title));
    }

    /// Guard (startup race): a bare stale `?tab=<pruned blank>&run=1` reload must still fall back
    /// to the survivor even when `IndexedDB.open` is slower than the auto-run. `run=1` carries no
    /// `#<query>` hash here, so the auto-run must NOT fire at the top level before reconciliation:
    /// if it did, its `postAll`/`saveHistory` would rewrite `history.state` to the bootstrap tab
    /// while `loadFromDb` was still opening, `stale_blank_reload` would stop matching the pruned
    /// blank tab, and the authoritative path would recreate and re-persist `Scratch`. Delaying
    /// `IndexedDB.open` by 30 ms makes that race deterministic. With the fix (every startup `run=1`
    /// deferred to `reconcileStartup`), the workspace still falls back to `Report` and `Scratch`
    /// stays pruned.
    {
        const r = await runScenario(js, {
            href: base + '?tab=Scratch&run=1',
            historyState: { tabId: 't7', tabName: 'Scratch' },
            openDelayMs: 30,
            seedTabs: [
                { id: 't7', title: 'Scratch', query: '', params: {}, result: null, lastSavedQuery: '' },
                { id: 't8', title: 'Report', query: 'SELECT 1', params: {}, result: { ran: true, query: 'SELECT 1', params: {} }, lastSavedQuery: 'SELECT 1' },
            ],
            seedMeta: { key: 'state', activeTabId: 't7', tabOrder: ['t7', 't8'], tabSeq: 8, tabTitleSeq: 2 },
        });
        check('stale-reload-run-race', 'the blank tab is not resurrected under a slow IndexedDB open',
            r.live.tabs.length === 1 && r.live.tabs[0].title === 'Report',
            r.live);
        check('stale-reload-run-race', 'no blank Scratch is re-persisted to IndexedDB',
            !r.persisted.some(p => p.title === 'Scratch'),
            r.persisted.map(p => p.title));
    }

    /// Guard (dirty-startup run leak): opening a `?run=1#<query>` auto-run link and typing into the
    /// bootstrap workspace before IndexedDB resolves makes reconciliation keep the LIVE tabs
    /// (`bootstrap_dirty`) and abandon the startup auto-run. The global `run_immediately` must be
    /// cleared when that path wins: otherwise it survives the merge, and the next `syncHistory`
    /// (e.g. switching into a clean, run-backed `Report` tab, whose `tabReflectsRun` is true) would
    /// recompute `run=1` as `run_immediately && tabReflectsRun(tab)` and re-stamp `?run=1` onto
    /// `Report`'s URL — so a later reload auto-executes a query the user never asked to auto-run.
    {
        const r = await runScenario(js, {
            href: base + '?run=1#' + encodeURIComponent('SELECT 111'),
            historyState: null,
            openDelayMs: 30,
            /// A genuine keystroke into the bootstrap editor while IndexedDB is still opening. The
            /// page's real synthetic edits carry `isTrusted === false` and are ignored by
            /// `markBootstrapDirty`; a plain event object with `isTrusted: true` mimics a real one.
            duringLoad: (sandbox) => {
                vm.runInContext(
                    "query_area.value = 'SELECT 999';" +
                    "query_area.dispatchEvent({ type: 'input', isTrusted: true });",
                    sandbox);
            },
            seedTabs: [
                { id: 't8', title: 'Report', query: 'SELECT 1', params: {}, result: { ran: true, query: 'SELECT 1', params: {} }, lastSavedQuery: 'SELECT 1' },
            ],
            seedMeta: { key: 'state', activeTabId: 't8', tabOrder: ['t8'], tabSeq: 8, tabTitleSeq: 2 },
        });
        check('dirty-startup-run-leak', 'the live typed tab and the restored Report both survive the merge',
            r.live.tabs.some(t => t.query === 'SELECT 999') && r.live.tabs.some(t => t.title === 'Report'),
            r.live);
        /// Now switch into the clean, run-backed Report, as the user would. With the leak,
        /// `syncHistory` re-stamps `run=1` onto its URL; with the fix it does not.
        const report = r.live.tabs.find(t => t.title === 'Report');
        await vm.runInContext(`switchToTab(${JSON.stringify(report.id)})`, r.sandbox);
        await sleep(50);
        const switched_url = new URL(r.sandbox.location.href);
        check('dirty-startup-run-leak', 'switching to the run-backed Report does not re-stamp run=1',
            switched_url.searchParams.get('run') === null && switched_url.searchParams.get('tab') === 'Report',
            r.sandbox.location.href);
    }

    /// Guard (dirty-startup adopt-name restamp): a fresh/shared `?tab=Scratch` link with no saved
    /// tabs, typed into before `IndexedDB` resolves. The trusted keystroke seeds the current entry
    /// via `refreshCurrentHistoryEntry` — `history.state` is still null at that point, so it does
    /// NOT bail — stamping the entry/URL with the live tab's PRE-rename default title (`Query A`).
    /// When reconciliation then adopts `url_tab_name` for that same live tab (no saved tabs to
    /// merge with), the entry/URL must be restamped to the newly adopted title too; otherwise the
    /// tab actually saved is `Scratch` while the current entry/URL still name `Query A`, and
    /// reloading (or navigating to) that entry would recreate a second `Query A` tab alongside it.
    {
        const r = await runScenario(js, {
            href: base + '?tab=Scratch',
            historyState: null,
            openDelayMs: 30,
            duringLoad: (sandbox) => {
                vm.runInContext(
                    "query_area.value = 'SELECT 999';" +
                    "query_area.dispatchEvent({ type: 'input', isTrusted: true });",
                    sandbox);
            },
            seedTabs: [],
            seedMeta: null,
        });
        check('dirty-startup-adopt-restamp', 'the live tab is renamed to Scratch and keeps the live edit',
            r.live.tabs.length === 1 && r.live.tabs[0].title === 'Scratch' && r.live.tabs[0].query === 'SELECT 999',
            r.live);
        const restamped_url = new URL(r.sandbox.location.href);
        check('dirty-startup-adopt-restamp', 'the current entry/URL is restamped to the adopted title',
            restamped_url.searchParams.get('tab') === 'Scratch',
            r.sandbox.location.href);
        check('dirty-startup-adopt-restamp', 'history.state.tabName matches the adopted title',
            r.sandbox.history.state && r.sandbox.history.state.tabName === 'Scratch',
            r.sandbox.history.state);
    }

    /// Guard (all-blank stale reload, live edit survives): reload a URL echoing a single, now-
    /// pruned blank saved tab (`?tab=Scratch`, `history.state` naming it), and type into the
    /// bootstrap editor before `IndexedDB` resolves. The persisted tab's id (`t7`) does not match
    /// the fresh bootstrap tab's id (`t1`), so `refreshCurrentHistoryEntry` bails without touching
    /// `history.state` — `bootstrap_dirty` is the only signal that a live edit is in flight. Since
    /// every saved tab is pruned, `savedTabs.length` is 0 and this reaches the same stale-echo
    /// cleanup branch as the `stale-reload-whitespace-hash` guard above; that branch must not win
    /// over a live, non-blank edit the way it does over the pruned tab's own stray blank text.
    {
        const r = await runScenario(js, {
            href: base + '?tab=Scratch',
            historyState: { tabId: 't7', tabName: 'Scratch' },
            openDelayMs: 30,
            duringLoad: (sandbox) => {
                vm.runInContext(
                    "query_area.value = 'SELECT 999';" +
                    "query_area.dispatchEvent({ type: 'input', isTrusted: true });",
                    sandbox);
            },
            seedTabs: [
                { id: 't7', title: 'Scratch', query: '', params: {}, result: null, lastSavedQuery: '' },
            ],
            seedMeta: { key: 'state', activeTabId: 't7', tabOrder: ['t7'], tabSeq: 7, tabTitleSeq: 1 },
        });
        check('dirty-startup-allblank-edit-survives', 'the live edit survives instead of being wiped',
            r.live.tabs.length === 1 && r.live.tabs[0].query === 'SELECT 999',
            r.live);
        check('dirty-startup-allblank-edit-survives', 'the live edit is what gets persisted, not a blank tab',
            r.persisted.some(p => p.query === 'SELECT 999') && !r.persisted.some(p => (p.query || '').trim() === ''),
            r.persisted.map(p => ({ id: p.id, query: p.query })));

        /// Guard (the same dirty all-blank stale echo, second reload): keeping the live workspace
        /// is not enough — the current entry/URL still belonged to the DROPPED blank tab
        /// (`?tab=Scratch`, `history.state.tabId === 't7'`), while what got persisted is the live
        /// tab under its own id/title. Reconciliation must re-own that entry for the live tab:
        /// otherwise the next reload finds nothing to prune (the live tab holds a real query), so
        /// `stale_blank_reload` no longer recognizes the leftover `?tab=`/`history.state` as stale,
        /// the named-URL path treats them as authoritative, and a fresh blank `Scratch` is
        /// recreated (and re-persisted) alongside the live tab.
        const live_tab = r.live.tabs[0];
        check('dirty-startup-allblank-entry-reowned', 'the current entry is re-owned by the live tab',
            r.sandbox.history.state && r.sandbox.history.state.tabId === live_tab.id,
            r.sandbox.history.state);
        check('dirty-startup-allblank-entry-reowned', 'the URL names the live tab, not the dropped blank one',
            new URL(r.sandbox.location.href).searchParams.get('tab') === live_tab.title,
            r.sandbox.location.href);
        /// Reload the workspace exactly as the browser would: same URL and `history.state` the
        /// first load left behind, seeded from what it persisted.
        const r2 = await runScenario(js, {
            href: r.sandbox.location.href,
            historyState: r.sandbox.history.state,
            seedTabs: r.persisted,
            seedMeta: r.persistedMeta,
        });
        check('dirty-startup-allblank-entry-reowned', 'the second reload does not recreate a blank tab under the dropped name',
            r2.live.tabs.length === 1 && r2.live.tabs[0].query === 'SELECT 999',
            r2.live);
        check('dirty-startup-allblank-entry-reowned', 'the second reload persists no blank tab',
            r2.persisted.length === 1 && !r2.persisted.some(p => (p.query || '').trim() === ''),
            r2.persisted.map(p => ({ id: p.id, title: p.title, query: p.query })));
    }

    /// Guard (dirty-startup merge, entry re-owned): the same dirty-startup race, but with a saved
    /// tab that SURVIVES the pruning. Reload `?tab=Scratch#<SELECT 111>` — `history.state` still
    /// names the blank, now-pruned `Scratch` tab from the previous session — and type `SELECT 999`
    /// into the bootstrap editor before `IndexedDB` resolves. Reconciliation takes the saved-tabs
    /// merge branch (`savedTabs.length` truthy AND `bootstrap_dirty`): it keeps the live edit and
    /// brings the surviving `Report` alongside it. The current entry still belonged to the dropped
    /// `Scratch` tab — the live edit could not claim it (`refreshCurrentHistoryEntry` bails on
    /// another tab's entry) — so it carries `Scratch`'s stale `#SELECT 111` hash and foreign
    /// `history.state`. Merely restamping `?tab=` would leave that stale hash in place: on the next
    /// reload startup treats it as an authoritative shared URL and overwrites the persisted
    /// `SELECT 999` back to `SELECT 111`. The merge branch must re-own the entry for the live tab,
    /// exactly as the all-blank branch does.
    {
        const stale_hash = Buffer.from('SELECT 111', 'utf8').toString('base64');
        const r = await runScenario(js, {
            href: base + '?tab=Scratch#' + stale_hash,
            historyState: { tabId: 't7', tabName: 'Scratch' },
            openDelayMs: 30,
            duringLoad: (sandbox) => {
                vm.runInContext(
                    "query_area.value = 'SELECT 999';" +
                    "query_area.dispatchEvent({ type: 'input', isTrusted: true });",
                    sandbox);
            },
            seedTabs: [
                { id: 't7', title: 'Scratch', query: '', params: {}, result: null, lastSavedQuery: '' },
                { id: 't8', title: 'Report', query: 'SELECT 1', params: {}, result: { ran: true, query: 'SELECT 1', params: {} }, lastSavedQuery: 'SELECT 1' },
            ],
            seedMeta: { key: 'state', activeTabId: 't7', tabOrder: ['t7', 't8'], tabSeq: 8, tabTitleSeq: 2 },
        });
        check('dirty-startup-merge-entry-reowned', 'the live typed tab and the restored Report both survive the merge',
            r.live.tabs.length === 2 && r.live.tabs.some(t => t.query === 'SELECT 999') && r.live.tabs.some(t => t.title === 'Report'),
            r.live);
        check('dirty-startup-merge-entry-reowned', 'the live edit is what gets persisted, alongside the surviving Report',
            r.persisted.some(p => p.query === 'SELECT 999') && r.persisted.some(p => p.query === 'SELECT 1')
                && !r.persisted.some(p => (p.query || '').trim() === ''),
            r.persisted.map(p => ({ id: p.id, title: p.title, query: p.query })));

        const live_tab = r.live.tabs.find(t => t.query === 'SELECT 999');
        check('dirty-startup-merge-entry-reowned', 'the current entry is re-owned by the live tab',
            r.sandbox.history.state && r.sandbox.history.state.tabId === live_tab.id,
            r.sandbox.history.state);
        const reowned_url = new URL(r.sandbox.location.href);
        check('dirty-startup-merge-entry-reowned', 'the URL hash carries the live query, not the dropped blank tab stale hash',
            Buffer.from(reowned_url.hash.slice(1), 'base64').toString('utf8') === 'SELECT 999'
                && reowned_url.searchParams.get('tab') === live_tab.title,
            r.sandbox.location.href);

        /// Reload the workspace exactly as the browser would: same URL and `history.state` the first
        /// load left behind, seeded from what it persisted. With the stale hash cleared, the URL is
        /// now the live tab's own `SELECT 999`, so nothing overwrites the persisted query.
        const r2 = await runScenario(js, {
            href: r.sandbox.location.href,
            historyState: r.sandbox.history.state,
            seedTabs: r.persisted,
            seedMeta: r.persistedMeta,
        });
        check('dirty-startup-merge-entry-reowned', 'the second reload keeps the persisted live query, not the stale hash',
            r2.live.tabs.some(t => t.query === 'SELECT 999') && !r2.live.tabs.some(t => t.query === 'SELECT 111'),
            r2.live);
        check('dirty-startup-merge-entry-reowned', 'the second reload persists the live query and Report, no stale or blank tab',
            r2.persisted.some(p => p.query === 'SELECT 999') && r2.persisted.some(p => p.query === 'SELECT 1')
                && !r2.persisted.some(p => p.query === 'SELECT 111') && !r2.persisted.some(p => (p.query || '').trim() === ''),
            r2.persisted.map(p => ({ id: p.id, title: p.title, query: p.query })));
    }

    /// Guard (no-saved-tabs adopt, entry re-owned + later edit survives): the analog of the merge/
    /// all-blank re-own races, but for the branch where NO saved tab survives the pruning and the URL
    /// carries an authoritative non-blank hash. Reload `?tab=Scratch#<SELECT 111>` from the stale
    /// history entry of a now-pruned blank `Scratch` tab; the adopt branch recreates the bootstrap
    /// tab from the hash and adopts the name. Unlike the dirty cases above there is no live edit
    /// during load — the second edit lands AFTER reconciliation completes. If the branch only
    /// restamped `?tab=`, the current entry would keep the dropped tab's foreign `history.state`
    /// (`tabId === 't7'`), so that later trusted edit could not re-own it (`refreshCurrentHistoryEntry`
    /// bails), and a reload before the debounced save would restore the stale `SELECT 111` and lose
    /// the edit. Reconciliation must re-own the entry for the recreated tab, exactly as the dirty
    /// branches do.
    {
        const stale_hash = Buffer.from('SELECT 111', 'utf8').toString('base64');
        const r = await runScenario(js, {
            href: base + '?tab=Scratch#' + stale_hash,
            historyState: { tabId: 't7', tabName: 'Scratch' },
            seedTabs: [
                { id: 't7', title: 'Scratch', query: '', params: {}, result: null, lastSavedQuery: '' },
            ],
            seedMeta: { key: 'state', activeTabId: 't7', tabOrder: ['t7'], tabSeq: 7, tabTitleSeq: 1 },
        });
        check('adopt-stale-reload-entry-reowned', 'the recreated tab adopts the name and the authoritative hash query',
            r.live.tabs.length === 1 && r.live.tabs[0].title === 'Scratch' && r.live.tabs[0].query === 'SELECT 111',
            r.live);
        const live_tab = r.live.tabs[0];
        check('adopt-stale-reload-entry-reowned', 'the current entry is re-owned by the recreated tab, not the dropped blank one',
            r.sandbox.history.state && r.sandbox.history.state.tabId === live_tab.id,
            r.sandbox.history.state);
        const adopted_url = new URL(r.sandbox.location.href);
        check('adopt-stale-reload-entry-reowned', 'the URL hash carries the adopted query and names the recreated tab',
            Buffer.from(adopted_url.hash.slice(1), 'base64').toString('utf8') === 'SELECT 111'
                && adopted_url.searchParams.get('tab') === live_tab.title,
            r.sandbox.location.href);

        /// Snapshot what the DB holds BEFORE the second edit — the reload models "before the debounced
        /// save flushes", so the DB must still carry the recreated `SELECT 111` tab, not `SELECT 999`.
        const db_before_edit = JSON.parse(JSON.stringify(r.persisted));
        /// A trusted keystroke AFTER reconciliation completes: with the entry re-owned this now folds
        /// into it (`refreshCurrentHistoryEntry` no longer bails), updating the entry/URL to the edit.
        vm.runInContext(
            "query_area.value = 'SELECT 999';" +
            "query_area.dispatchEvent({ type: 'input', isTrusted: true });",
            r.sandbox);
        const edited_url = new URL(r.sandbox.location.href);
        check('adopt-stale-reload-entry-reowned', 'a later trusted edit re-owns the entry instead of bailing',
            Buffer.from(edited_url.hash.slice(1), 'base64').toString('utf8') === 'SELECT 999'
                && r.sandbox.history.state && r.sandbox.history.state.query === 'SELECT 999'
                && r.sandbox.history.state.tabId === live_tab.id,
            { hash: r.sandbox.location.href, state: r.sandbox.history.state });

        /// Reload before the edit is persisted: the DB still holds `SELECT 111`, but the entry now
        /// holds `SELECT 999`. Startup must keep the newer edit from the entry, not the stale DB/hash.
        const r2 = await runScenario(js, {
            href: r.sandbox.location.href,
            historyState: r.sandbox.history.state,
            seedTabs: db_before_edit,
            seedMeta: r.persistedMeta,
        });
        check('adopt-stale-reload-entry-reowned', 'the reload keeps the newest edit, not the stale hash',
            r2.live.tabs.some(t => t.query === 'SELECT 999') && !r2.live.tabs.some(t => t.query === 'SELECT 111'),
            r2.live);
        check('adopt-stale-reload-entry-reowned', 'the reload persists the newest edit, no stale or blank tab',
            r2.persisted.some(p => p.query === 'SELECT 999') && !r2.persisted.some(p => p.query === 'SELECT 111')
                && !r2.persisted.some(p => (p.query || '').trim() === ''),
            r2.persisted.map(p => ({ id: p.id, title: p.title, query: p.query })));
    }

    /// Guard (no-saved-tabs adopt, authoritative `param_*` preserved): the adopt branch deliberately
    /// defers its `refreshCurrentHistoryEntry` re-own to the tail so it runs AFTER the URL's
    /// placeholder parameters are reconciled into the recreated tab — the rebuilt entry/URL must
    /// carry the adopted title, query AND the `param_*` values together. Open a fresh/shared link
    /// `?tab=Report&param_x=42#<SELECT {x:Int32}>` on an empty workspace (no saved tabs, no history
    /// state), so reconciliation takes the no-saved-tabs adopt branch: it recreates the bootstrap
    /// tab, adopts the name, then folds `param_x=42` into the tab before the tail re-own rebuilds the
    /// entry. If someone moved that re-own back before the URL-parameter reconciliation, or stopped
    /// copying the reconciled parameters into `tab.params`, the rebuilt entry/URL would silently lose
    /// `param_x` and the placeholder query would load/run with an empty binding. Assert both the
    /// first load and a reload preserve `x = 42` in the URL, `history.state` and IndexedDB.
    {
        const param_query = 'SELECT {x:Int32}';
        const param_hash = Buffer.from(param_query, 'utf8').toString('base64');
        const r = await runScenario(js, {
            href: base + '?tab=Report&param_x=42#' + param_hash,
            historyState: null,
            seedTabs: [],
            seedMeta: null,
        });
        check('adopt-param-authoritative', 'the adopt branch recreates the tab with the shared name and placeholder query',
            r.live.tabs.length === 1 && r.live.tabs[0].title === 'Report' && r.live.tabs[0].query === param_query,
            r.live);
        const live_tab = r.live.tabs[0];
        const adopted_url = new URL(r.sandbox.location.href);
        check('adopt-param-authoritative', 'the re-owned URL carries the placeholder value, the adopted name and the query hash',
            adopted_url.searchParams.get('param_x') === '42'
                && adopted_url.searchParams.get('tab') === 'Report'
                && Buffer.from(adopted_url.hash.slice(1), 'base64').toString('utf8') === param_query,
            r.sandbox.location.href);
        check('adopt-param-authoritative', 'the re-owned history entry belongs to the recreated tab and carries the reconciled param',
            r.sandbox.history.state && r.sandbox.history.state.tabId === live_tab.id
                && r.sandbox.history.state.params && r.sandbox.history.state.params.x === '42',
            r.sandbox.history.state);
        check('adopt-param-authoritative', 'the reconciled param is persisted with the recreated tab',
            r.persisted.length === 1 && r.persisted[0].params && r.persisted[0].params.x === '42',
            r.persisted.map(p => ({ id: p.id, title: p.title, params: p.params })));

        /// Reload the shared link exactly as the browser would: same URL and `history.state` the
        /// first load left behind, seeded from what it persisted. The named tab now matches, so
        /// reconciliation activates it and re-applies the URL parameters, which must still resolve to
        /// the same `x = 42` rather than being dropped or reset to the empty default.
        const r2 = await runScenario(js, {
            href: r.sandbox.location.href,
            historyState: r.sandbox.history.state,
            seedTabs: r.persisted,
            seedMeta: r.persistedMeta,
        });
        const reloaded_url = new URL(r2.sandbox.location.href);
        check('adopt-param-authoritative', 'the reload keeps a single Report tab with the placeholder query',
            r2.live.tabs.length === 1 && r2.live.tabs[0].title === 'Report' && r2.live.tabs[0].query === param_query,
            r2.live);
        check('adopt-param-authoritative', 'the reload preserves the param value in the URL, history state and IndexedDB',
            reloaded_url.searchParams.get('param_x') === '42'
                && r2.sandbox.history.state && r2.sandbox.history.state.params && r2.sandbox.history.state.params.x === '42'
                && r2.persisted.length === 1 && r2.persisted[0].params && r2.persisted[0].params.x === '42',
            { url: r2.sandbox.location.href, state: r2.sandbox.history.state, persisted: r2.persisted.map(p => p.params) });
    }

    if (failures) {
        console.log(`${failures} check(s) FAILED`);
        process.exit(1);
    }
    console.log('All scenarios passed');
}

main().catch((e) => {
    console.log('HARNESS ERROR: ' + (e && e.stack || e));
    process.exit(1);
});
