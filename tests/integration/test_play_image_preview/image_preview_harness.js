#!/usr/bin/env node
/// Executable regression harness for the Web UI image-preview hover gesture (`attachImagePreview`,
/// `showImagePreview`, `hideImagePreview`, `setImagePreviewModifierHeld`) and the URL-qualification
/// gate that decides which result cells become previewable (`QueryResultElement._renderCell`).
///
/// The preview fetches a URL taken from untrusted query results, so its contract is
/// security-sensitive:
///   * only URLs whose parsed `pathname` ends in an image extension may preview - a query string or
///     fragment ending in `.png` (e.g. `https://host/restart#x.png`) or a non-`http`/`https` scheme
///     must NOT, because the browser requests the non-image path;
///   * plain hover (no modifier) must NEVER assign `src`, i.e. must never issue an outbound request;
///   * only `Ctrl` (or `Cmd` on Mac) + hover may assign `src`;
///   * `scroll` (including a scroll INSIDE the `query-result` shadow tree), `clear`, `blur` and
///     modifier-release must tear the preview down - clear `src` and suppress a load that finishes
///     after the preview was dismissed.
///
/// So that the suite proves the production wiring rather than a re-statement of it, the security
/// scenarios drive the real code paths: the gate cases run `QueryResultElement._renderCell` and the
/// shadow-scroll case builds a real `query-result` shadow root and fires `scroll` from an inner
/// scroller, exercising the capture-phase `this.shadowRoot` listener through actual event routing.
///
/// The stateless suite has no JavaScript runtime, so the contract is driven by a Node.js harness
/// executed inside the `clickhouse/mysql-js-client` container (node:22-alpine): it fetches `/play`
/// from a real server, runs the extracted page script in a `vm` context with a stubbed browser
/// environment, then dispatches the hover/keyboard/scroll events for each scenario and asserts what
/// the reused `#image-preview` `<img>` element does.
///
/// Can also be run standalone against a checkout for development:
///   node image_preview_harness.js programs/server/play.html
///
/// Usage: node image_preview_harness.js <path-or-url-of-play.html>
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

/// Fire the listeners registered on `node` for `ev.type` in a given DOM propagation phase:
///   * 'capture' - only capturing listeners (registered with a truthy third argument);
///   * 'bubble'  - only bubbling listeners;
///   * 'target'  - both, plus the legacy `on<type>` handler (the element is AT_TARGET).
/// This is what lets a capture-phase listener on an ancestor (e.g. the shadow root) see a `scroll`
/// dispatched on a descendant, exactly as a browser routes it.
function firePhaseListeners(node, ev, phase) {
    const listeners = node.__listeners;
    if (listeners) {
        for (const entry of (listeners.get(ev.type) || []).slice()) {
            if (phase === 'capture' && !entry.capture) continue;
            if (phase === 'bubble' && entry.capture) continue;
            try { Object.defineProperty(ev, 'currentTarget', { value: node, configurable: true }); } catch (e) { /* set */ }
            entry.fn.call(node, ev);
        }
    }
    if (phase !== 'capture') {
        const handler = node['on' + ev.type];
        if (typeof handler === 'function') {
            try { Object.defineProperty(ev, 'currentTarget', { value: node, configurable: true }); } catch (e) { /* set */ }
            handler.call(node, ev);
        }
    }
}

/// Fire a custom-element lifecycle callback (`connectedCallback` / `disconnectedCallback`) on `node`
/// and its light-DOM descendants. Only real custom elements (a constructed `QueryResultElement`)
/// define these; plain stubs do not, so this is a no-op for them. Used so removing a tab panel from
/// the DOM runs the genuine `QueryResultElement.disconnectedCallback` (the tab-removal teardown).
function fireLifecycle(node, name) {
    if (!node || node.nodeType !== 1) return;
    for (const c of (node.children || []).slice()) fireLifecycle(c, name);
    if (typeof node[name] === 'function') node[name]();
}

/// Install element behavior onto `el`. Used both for plain stub elements (`makeElement`) and as the
/// body of the `HTMLElement` base class, so a real `QueryResultElement` can be constructed and its
/// production methods (`_renderCell`, the constructor's shadow-root listeners) actually run.
function installElementBehavior(el, tag) {
    const listeners = new Map();
    const attributes = new Map();
    const byId = new Map();
    /// A real `<img>` reflects the `src` attribute in the `src` property and aborts the load when
    /// `src` is removed. Track every non-empty assignment (each is one outbound request) so a
    /// scenario can assert whether hovering fetched, and clear the property on `removeAttribute`.
    let src_value = '';
    const src_assignments = [];

    el.tagName = String(tag || 'div').toUpperCase();
    el.nodeType = 1;
    el.id = '';
    el.style = makeStyle();
    el.classList = makeClassList();
    el.dataset = {};
    el.children = [];
    el.childNodes = [];
    el.parentNode = null;
    el.parentElement = null;
    el.firstChild = null;
    el.lastChild = null;
    el.shadowRoot = null;
    el.value = '';
    el.textContent = '';
    el.innerHTML = '';
    el.innerText = '';
    el.title = '';
    el.placeholder = '';
    el.className = '';
    el.name = '';
    el.type = '';
    el.href = '';
    el.alt = '';
    el.hidden = false;
    /// An <img> that has not finished loading: keeps `showImagePreview` on the fetch-then-reveal
    /// path rather than the already-loaded fast path until a scenario marks it complete.
    el.complete = false;
    el.naturalWidth = 0;
    el.onload = null;
    el.onerror = null;
    /// The URLs `src` was assigned a non-empty value, i.e. the outbound requests issued.
    el.src_assignments = src_assignments;
    /// Registered event listeners, exposed so the propagation dispatcher can fire ancestor listeners.
    el.__listeners = listeners;

    el.addEventListener = function (type, fn, opts) {
        const capture = opts === true || (opts && opts.capture) || false;
        if (!listeners.has(type)) listeners.set(type, []);
        listeners.get(type).push({ fn, capture });
    };
    el.removeEventListener = function (type, fn, opts) {
        const capture = opts === true || (opts && opts.capture) || false;
        const l = listeners.get(type);
        if (l) {
            const i = l.findIndex(x => x.fn === fn && x.capture === capture);
            if (i !== -1) l.splice(i, 1);
        }
    };
    /// Dispatch with real capture -> target -> bubble propagation up the `parentNode` chain, so a
    /// capture-phase listener on an ancestor (the shadow root) sees a non-bubbling `scroll` fired on
    /// a descendant. `scroll`/`mouseenter` do not bubble, so bubble phase runs only for `bubbles`.
    el.dispatchEvent = function (ev) {
        try { Object.defineProperty(ev, 'target', { value: el, configurable: true }); } catch (e) { /* set */ }
        const path = [];
        for (let n = el; n; n = n.parentNode) path.push(n);
        for (let i = path.length - 1; i >= 1; i--) firePhaseListeners(path[i], ev, 'capture');
        firePhaseListeners(el, ev, 'target');
        if (ev.bubbles) for (let i = 1; i < path.length; i++) firePhaseListeners(path[i], ev, 'bubble');
        return true;
    };
    el.appendChild = function (c) {
        el.children.push(c);
        el.childNodes.push(c);
        c.parentNode = el;
        c.parentElement = el;
        el.firstChild = el.children[0];
        el.lastChild = c;
        fireLifecycle(c, 'connectedCallback');
        return c;
    };
    el.removeChild = function (c) {
        el.children = el.children.filter(x => x !== c);
        el.childNodes = el.childNodes.filter(x => x !== c);
        el.firstChild = el.children[0] || null;
        el.lastChild = el.children[el.children.length - 1] || null;
        c.parentNode = null;
        c.parentElement = null;
        fireLifecycle(c, 'disconnectedCallback');
        return c;
    };
    el.insertBefore = function (c, ref) {
        const i = el.children.indexOf(ref);
        if (i === -1) return el.appendChild(c);
        el.children.splice(i, 0, c);
        el.childNodes.splice(i, 0, c);
        c.parentNode = el;
        c.parentElement = el;
        el.firstChild = el.children[0];
        fireLifecycle(c, 'connectedCallback');
        return c;
    };
    el.replaceChildren = function (...cs) {
        const old = el.children.slice();
        el.children = [...cs];
        el.childNodes = [...cs];
        for (const c of cs) { c.parentNode = el; c.parentElement = el; }
        el.firstChild = el.children[0] || null;
        el.lastChild = el.children[el.children.length - 1] || null;
        for (const c of old) { if (!cs.includes(c)) { c.parentNode = null; c.parentElement = null; fireLifecycle(c, 'disconnectedCallback'); } }
        for (const c of cs) fireLifecycle(c, 'connectedCallback');
    };
    el.remove = function () { if (el.parentNode) el.parentNode.removeChild(el); };
    el.setAttribute = function (k, v) {
        attributes.set(k, String(v));
        if (k === 'id') el.id = String(v);
        if (k === 'src') { src_value = String(v); if (src_value) src_assignments.push(src_value); }
    };
    el.getAttribute = function (k) { return attributes.has(k) ? attributes.get(k) : null; };
    el.removeAttribute = function (k) {
        attributes.delete(k);
        /// Removing `src` aborts the in-flight load and empties the property, exactly as a
        /// browser does; this is what `hideImagePreview` relies on to stop a background fetch.
        if (k === 'src') { src_value = ''; }
    };
    el.hasAttribute = function (k) { return attributes.has(k); };
    el.focus = function () {};
    el.blur = function () {};
    el.click = function () { el.dispatchEvent({ type: 'click' }); };
    el.select = function () {};
    el.setSelectionRange = function () {};
    el.getBoundingClientRect = function () { return { top: 0, left: 0, right: 0, bottom: 0, width: 0, height: 0, x: 0, y: 0 }; };
    el.getClientRects = function () { return []; };
    el.querySelector = function () { return null; };
    el.querySelectorAll = function () { return []; };
    el.closest = function () { return null; };
    el.matches = function () { return false; };
    /// A real `contains`: true for the element itself or any light-DOM descendant. `setActivePanel`
    /// uses `panel.contains(image_preview_owner)` to keep a preview owned by the newly active panel
    /// and dismiss one owned by a panel switched away from, so this must walk the subtree.
    el.contains = function (target) {
        if (target === el) return true;
        for (const c of el.children) { if (c === target || (c.contains && c.contains(target))) return true; }
        return false;
    };
    el.scrollIntoView = function () {};
    el.scrollTo = function () {};
    el.scroll = function () {};
    el.cloneNode = function () { return makeElement(el.tagName); };
    el.insertAdjacentElement = function () {};
    el.insertAdjacentHTML = function () {};
    el.insertAdjacentText = function () {};
    el.getContext = function () { return null; };
    el.getElementById = function (id) {
        if (!byId.has(id)) { const c = makeElement('div'); c.id = id; byId.set(id, c); }
        return byId.get(id);
    };
    /// A real `attachShadow` sets `el.shadowRoot`; the `query-result` constructor relies on it to
    /// register its capture-phase scroll/click listeners on the shadow root.
    el.attachShadow = function () {
        const sr = makeElement('#shadow-root');
        sr.nodeType = 11;
        sr.host = el;
        sr.parentNode = null;
        el.shadowRoot = sr;
        return sr;
    };

    /// `src` reflects the attribute both ways: assigning the property is an outbound request, and
    /// `removeAttribute('src')` empties it.
    Object.defineProperty(el, 'src', {
        get() { return src_value; },
        set(v) { src_value = String(v); if (src_value) src_assignments.push(src_value); },
        configurable: true,
        enumerable: true,
    });
    return el;
}

function makeElement(tag) {
    const el = installElementBehavior({}, tag);
    /// Inert versions of the custom-element methods the page script calls on plain stub elements (the
    /// custom-element upgrade never runs for stubs). NOT installed on the real `HTMLElement` base, so
    /// they do not shadow the genuine `QueryResultElement` methods when one is constructed.
    Object.assign(el, {
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
    });
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

/// ----- Minimal IndexedDB fake (only what startup `openDb`/`loadFromDb`/`persist` touch) -------

function makeIndexedDB() {
    const stores = new Map();
    stores.set('tabs', { keyPath: 'id', data: new Map() });
    stores.set('meta', { keyPath: 'key', data: new Map() });

    function makeStoreHandle(name) {
        const s = stores.get(name);
        return {
            getAll() { return { result: [...s.data.values()] }; },
            get(key) { return { result: s.data.get(key) }; },
            put(obj) { s.data.set(obj[s.keyPath], obj); return { result: obj[s.keyPath] }; },
            clear() { s.data.clear(); return { result: undefined }; },
            delete(key) { s.data.delete(key); return { result: undefined }; },
        };
    }

    return {
        open() {
            const req = { onupgradeneeded: null, onsuccess: null, onerror: null, result: null };
            setTimeout(() => {
                req.result = {
                    objectStoreNames: { contains: (n) => stores.has(n) },
                    createObjectStore(n, opts) {
                        if (!stores.has(n)) stores.set(n, { keyPath: opts.keyPath, data: new Map() });
                        return makeStoreHandle(n);
                    },
                    transaction() {
                        const tx = { oncomplete: null, onerror: null, onabort: null };
                        tx.objectStore = (n) => makeStoreHandle(n);
                        setTimeout(() => { if (tx.oncomplete) tx.oncomplete(); }, 0);
                        return tx;
                    },
                    close() {},
                };
                if (req.onsuccess) req.onsuccess();
            }, 0);
            return req;
        },
    };
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
        assign() {}, replace() {}, reload() {},
        _apply(url) { u.href = new URL(url, u.href).href; },
    };
}

function makeHistory(location) {
    return {
        state: null,
        length: 1,
        replaceState(state, title, url) { this.state = state; if (url != null) location._apply(String(url)); },
        pushState(state, title, url) { this.state = state; this.length++; if (url != null) location._apply(String(url)); },
        back() {}, forward() {}, go() {},
    };
}

/// ----- Context assembly -------------------------------------------------------------------

function makeWindowEventTarget() {
    /// `window`/`self`/`globalThis` is the sandbox itself, so it needs real event dispatch: the
    /// image-preview code registers `keydown`/`keyup`/`blur` on `window`, and the scenarios fire
    /// them to arm and release the modifier gate.
    const listeners = new Map();
    return {
        addEventListener(type, fn) {
            if (!listeners.has(type)) listeners.set(type, []);
            listeners.get(type).push(fn);
        },
        removeEventListener(type, fn) {
            const l = listeners.get(type);
            if (l) { const i = l.indexOf(fn); if (i !== -1) l.splice(i, 1); }
        },
        dispatchEvent(ev) {
            for (const fn of (listeners.get(ev.type) || []).slice()) fn.call(this, ev);
            return true;
        },
    };
}

function makeContext() {
    const document = makeDocument();
    const location = makeLocation('http://127.0.0.1:8123/play');
    const history = makeHistory(location);
    const winEvents = makeWindowEventTarget();
    let fetch_count = 0;

    const sandbox = {
        document,
        location,
        history,
        indexedDB: makeIndexedDB(),
        localStorage: makeStorage(),
        sessionStorage: makeStorage(),
        navigator: {
            clipboard: { writeText: async () => {}, readText: async () => '' },
            platform: 'Linux x86_64',
            language: 'en-US',
            userAgent: 'play-image-preview-harness',
        },
        /// No network. Count calls so a scenario can assert that plain hover issues no request even
        /// via `fetch` (the preview uses `img.src`, but this guards against any future change).
        fetch: async () => {
            fetch_count++;
            return {
                ok: false, status: 503, statusText: 'harness: network disabled',
                headers: { get: () => null }, text: async () => '', json: async () => ({}),
            };
        },
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
        /// The tabs code derives a per-page-load id from `crypto.getRandomValues` at top level.
        crypto: globalThis.crypto,
        /// A functional element base so `new QueryResultElement()` runs the real constructor
        /// (`attachShadow`, the shadow-root scroll/click listeners) and its `_renderCell`.
        HTMLElement: class HTMLElement { constructor() { installElementBehavior(this, 'html-element'); } },
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
        addEventListener: (...a) => winEvents.addEventListener(...a),
        removeEventListener: (...a) => winEvents.removeEventListener(...a),
        dispatchEvent: (ev) => winEvents.dispatchEvent(ev),
        WebAssembly,
    };
    sandbox.window = sandbox;
    sandbox.self = sandbox;
    sandbox.globalThis = sandbox;
    vm.createContext(sandbox);
    return { sandbox, fetch_count: () => fetch_count };
}

/// ----- Scenario driver ----------------------------------------------------------------------

function extractScript(html) {
    const blocks = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)].map(m => m[1]);
    if (!blocks.length) throw new Error('no <script> block found in play.html');
    return blocks.reduce((a, b) => (a.length >= b.length ? a : b));
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

/// Boot a fresh page: run the extracted script and let its async startup settle so nothing races the
/// scenario. Returns the sandbox, a `run(code)` helper, and the network counter.
async function bootPage(js) {
    const { sandbox, fetch_count } = makeContext();
    vm.runInContext(js, sandbox, { filename: 'play.html.js' });
    /// The image-preview functions and window listeners are defined synchronously at top level; let
    /// the async startup (IndexedDB open, reconciliation) settle so nothing races the scenario.
    await sleep(20);
    return { sandbox, fetchCount: fetch_count, run: (code) => vm.runInContext(code, sandbox) };
}

/// Boot a page and install a single image link that would preview `url`, wired exactly as the
/// result renderer does (`attachImagePreview`). Returns helpers to drive the gesture and read state.
async function boot(js, url) {
    const page = await bootPage(js);
    const { run } = page;

    /// Build the link and wire the preview onto it, the same call the result renderer makes.
    run(`globalThis.__link = document.createElement('a');
         globalThis.__link.href = ${JSON.stringify(url)};
         attachImagePreview(__link, ${JSON.stringify(url)});`);

    return {
        sandbox: page.sandbox,
        fetchCount: page.fetchCount,
        /// Fire a pointer event on the link (mouseenter / mousemove / mouseleave).
        pointer: (type, x = 10, y = 10) =>
            run(`__link.dispatchEvent({ type: ${JSON.stringify(type)}, clientX: ${x}, clientY: ${y} });`),
        /// Press / release the modifier on the window (Control or Meta), or blur the window.
        keydown: (key) => run(`window.dispatchEvent({ type: 'keydown', key: ${JSON.stringify(key)} });`),
        keyup: (key) => run(`window.dispatchEvent({ type: 'keyup', key: ${JSON.stringify(key)} });`),
        blur: () => run(`window.dispatchEvent({ type: 'blur' });`),
        /// Scroll the document (the global capture-phase handler dismisses the preview).
        scrollDocument: () => run(`document.dispatchEvent({ type: 'scroll' });`),
        clear: () => run(`clear();`),
        /// Capture the `<img>`'s current load handler so a scenario can deliver a late load after
        /// the preview has been torn down and assert it stays hidden.
        captureLoadHandler: () => run(`(globalThis.__late = image_preview && image_preview.onload, undefined)`),
        deliverLateLoad: () => run(`if (typeof __late === 'function') __late();`),
        /// Mark the current image as fully loaded (drives `showImagePreview`'s already-loaded path).
        markLoaded: () => run(`if (image_preview) { image_preview.complete = true; image_preview.naturalWidth = 64; }`),
        /// Fire the current image's load event (reveals the preview if not suppressed).
        fireLoad: () => run(`if (image_preview && typeof image_preview.onload === 'function') image_preview.onload();`),
        /// Snapshot of the reused preview element (null until `showImagePreview` first runs).
        state: () => JSON.parse(run(
            `JSON.stringify(image_preview === null ? null : {
                src: image_preview.src,
                display: image_preview.style.display,
                dataUrl: image_preview.dataset.url === undefined ? null : image_preview.dataset.url,
                assignments: image_preview.src_assignments.length,
                hasOnload: typeof image_preview.onload === 'function',
             })`)),
    };
}

/// Boot a page and build a real ACTIVE tab through the production tab machinery, whose result holds a
/// hovered, revealed image preview OWNED by that tab's result. Uses a genuine `QueryResultElement`
/// (so its real `clear`/`disconnectedCallback` run), `_renderCell` for the URL, the panel appended to
/// `resultPanelsEl`, and the tab pushed into `tabs` as active. A following production teardown
/// (`clearPanel`, `setActivePanel`, `panel.remove()`) can then be asserted to dismiss (or, for a
/// different tab, NOT dismiss) the preview. Returns `{ run }`; `installBackgroundTab` adds a second,
/// non-hovered real tab and returns its id via `globalThis.__bgId`.
async function bootRealTab(js, url) {
    const page = await bootPage(js);
    const { run } = page;
    run(`(function () {
        globalThis.__find = function find(n, pred) {
            if (!n) return null;
            if (pred(n)) return n;
            for (const ch of (n.children || [])) { const r = find(ch, pred); if (r) return r; }
            return null;
        };
        globalThis.__makeResultTab = function (title) {
            const qr = new QueryResultElement();
            const panel = document.createElement('div');
            panel.className = 'tab-panel';
            panel.appendChild(qr);                 // panel.contains(qr) is true; fires connectedCallback
            resultPanelsEl.appendChild(panel);
            const tab = makeTab(title, '');
            tab.panel = panel; tab.resultEl = qr;
            tabs.push(tab);
            return tab;
        };
        const tab = __makeResultTab('Active');
        activeTabId = tab.id;
        for (const t of tabs) if (t.panel) t.panel.hidden = (t !== tab);
        /// Render the image cell through the production renderer and hover it with the modifier held,
        /// so the preview is created, revealed, and anchored to this tab's result (image_preview_owner).
        const td = tab.resultEl._renderCell('c', ${JSON.stringify(url)});
        tab.resultEl.shadowRoot.appendChild(td);
        const link = __find(td, n => n.tagName === 'A');
        setImagePreviewModifierHeld(true);
        link.dispatchEvent({ type: 'mouseenter', clientX: 5, clientY: 5 });
        if (image_preview && typeof image_preview.onload === 'function') image_preview.onload();
    })()`);
    return { page, run };
}

/// Add a second, non-hovered real result tab (hidden background panel). Its id is in `globalThis.__bgId`.
function installBackgroundTab(run) {
    run(`(function () {
        const bg = __makeResultTab('Background');
        bg.panel.hidden = true;
        globalThis.__bgId = bg.id;
    })()`);
}

/// Snapshot the shared preview element plus whether it is still anchored to an owner.
function ownedState(run) {
    return JSON.parse(run(`JSON.stringify(image_preview === null ? null : {
        src: image_preview.src,
        display: image_preview.style.display,
        owner: image_preview_owner === null ? null : 'set',
    })`));
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
        console.error('usage: node image_preview_harness.js <path-or-url-of-play.html>');
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
    const url = 'http://example.test/cat.png';

    /// Contract 0: the URL-qualification gate. Which cells become previewable is decided in the
    /// production cell renderer `QueryResultElement._renderCell` (`^https?://\S+$`, then the image
    /// extension tested against `new URL(text).pathname`), so drive that path - not a hand-built
    /// link - and assert whether hovering with the modifier held issues a request. This catches a
    /// regression that reintroduced the fragment/query bypass or previewed a non-`http(s)` URL.
    {
        const cases = [
            { v: 'http://example.test/cat.png', link: true, preview: true, why: 'plain image URL' },
            { v: 'https://example.test/pic.jpeg?w=64', link: true, preview: true, why: 'query string after an image path' },
            { v: 'https://example.test/pic.webp#top', link: true, preview: true, why: 'fragment after an image path' },
            { v: 'https://host/restart#x.png', link: true, preview: false, why: 'image extension only in the fragment' },
            { v: 'https://host/restart?x.png', link: true, preview: false, why: 'image extension only in the query' },
            { v: 'ftp://host/image.png', link: false, preview: false, why: 'a non-http(s) scheme' },
            { v: 'not a url at all', link: false, preview: false, why: 'plain text' },
        ];
        const previewTitle = 'Ctrl+hover (Cmd+hover on Mac) to preview this image';
        for (const c of cases) {
            const page = await bootPage(js);
            const res = JSON.parse(page.run(`
              (function () {
                  const qr = new QueryResultElement();
                  const td = qr._renderCell('c', ${JSON.stringify(c.v)});
                  function findLink(n) {
                      if (!n || !n.tagName) return null;
                      if (n.tagName === 'A') return n;
                      for (const ch of (n.children || [])) { const r = findLink(ch); if (r) return r; }
                      return null;
                  }
                  const a = findLink(td);
                  let fetched = false;
                  if (a) {
                      setImagePreviewModifierHeld(true);          // hold Ctrl before hovering
                      a.dispatchEvent({ type: 'mouseenter', clientX: 10, clientY: 10 });
                      a.dispatchEvent({ type: 'mousemove', clientX: 12, clientY: 12 });
                      fetched = !!(image_preview && image_preview.src_assignments.length > 0);
                      setImagePreviewModifierHeld(false);
                  }
                  return JSON.stringify({ link: !!a, title: a ? a.title : null, fetched });
              })()`));
            check('url-gate', `${c.why} is ${c.link ? '' : 'not '}linkified`, res.link === c.link, res);
            check('url-gate', `${c.why} ${c.preview ? 'previews' : 'does not preview'} on Ctrl+hover`,
                res.fetched === c.preview, res);
            if (c.preview) {
                check('url-gate', `${c.why} advertises the gesture in its title`,
                    res.title === previewTitle, res);
            }
        }
    }

    /// Contract 1: plain hover (no modifier) must never fetch. Moving the cursor over an image link
    /// - even lingering on it - must not create the preview element or assign `src`.
    {
        const h = await boot(js, url);
        /// Baseline the network after startup (the page issues its own `fetch` calls on load), so the
        /// check measures only what hovering does.
        const fetches_before = h.fetchCount();
        h.pointer('mouseenter');
        h.pointer('mousemove');
        h.pointer('mousemove', 20, 20);
        check('plain-hover', 'no preview element is created and no request is issued',
            h.state() === null, h.state());
        check('plain-hover', 'hovering issues no fetch() call',
            h.fetchCount() === fetches_before, { before: fetches_before, after: h.fetchCount() });
    }

    /// Contract 2: Ctrl + hover fetches exactly once, keeps the preview hidden until the image loads,
    /// then reveals it; further movement over the same link does not refetch.
    {
        const h = await boot(js, url);
        h.keydown('Control');
        h.pointer('mouseenter');
        let s = h.state();
        check('ctrl-hover', 'src is assigned to the hovered URL', s && s.src === url, s);
        check('ctrl-hover', 'exactly one request is issued', s && s.assignments === 1, s);
        check('ctrl-hover', 'preview stays hidden until the image loads', s && s.display === 'none', s);
        h.fireLoad();
        s = h.state();
        check('ctrl-hover', 'preview is revealed once the image loads', s && s.display === 'block', s);
        h.pointer('mousemove', 30, 30);
        s = h.state();
        check('ctrl-hover', 'moving over the same link does not refetch', s && s.assignments === 1, s);
    }

    /// Contract 2b: the Mac modifier (Cmd / `Meta`) arms the gate just like `Control`.
    {
        const h = await boot(js, url);
        h.keydown('Meta');
        h.pointer('mouseenter');
        const s = h.state();
        check('cmd-hover', 'Cmd + hover assigns src', s && s.src === url && s.assignments === 1, s);
    }

    /// Contract 3: scroll tears the preview down - clears src and suppresses a late load. Scrolling
    /// a result surface can move the link out from under a stationary cursor without a `mouseleave`.
    {
        const h = await boot(js, url);
        h.keydown('Control');
        h.pointer('mouseenter');
        h.captureLoadHandler();
        h.scrollDocument();
        let s = h.state();
        check('scroll-teardown', 'src is cleared on scroll', s && s.src === '', s);
        check('scroll-teardown', 'preview is hidden and its load handler detached',
            s && s.display === 'none' && !s.hasOnload, s);
        h.deliverLateLoad();
        s = h.state();
        check('scroll-teardown', 'a load arriving after scroll does not reveal the preview',
            s && s.display === 'none', s);
    }

    /// Contract 3b: rerunning a query tears the result cell down through `clear()`, which must also
    /// dismiss the preview (no `mouseleave`/`scroll` is delivered on that path).
    {
        const h = await boot(js, url);
        h.keydown('Control');
        h.pointer('mouseenter');
        h.captureLoadHandler();
        h.clear();
        let s = h.state();
        check('clear-teardown', 'src is cleared when the result is cleared', s && s.src === '', s);
        check('clear-teardown', 'preview is hidden after clear', s && s.display === 'none', s);
        h.deliverLateLoad();
        s = h.state();
        check('clear-teardown', 'a load arriving after clear does not reveal the preview',
            s && s.display === 'none', s);
    }

    /// Contract 3c: losing the window focus (`blur`) or releasing the modifier (`keyup`) releases the
    /// gate and dismisses the preview, so it does not stay pinned while the modifier is "stuck".
    {
        const h = await boot(js, url);
        h.keydown('Control');
        h.pointer('mouseenter');
        h.captureLoadHandler();
        h.blur();
        let s = h.state();
        check('blur-teardown', 'blur clears src and hides the preview',
            s && s.src === '' && s.display === 'none', s);
        h.deliverLateLoad();
        s = h.state();
        check('blur-teardown', 'a load arriving after blur does not reveal the preview',
            s && s.display === 'none', s);
    }

    {
        const h = await boot(js, url);
        h.keydown('Control');
        h.pointer('mouseenter');
        h.keyup('Control');
        const s = h.state();
        check('modifier-release', 'releasing the modifier clears src and hides the preview',
            s && s.src === '' && s.display === 'none', s);
    }

    /// Contract 3d: a scroll INSIDE the `query-result` shadow tree also dismisses the preview. An
    /// expanded `.td-selected .cell-content` is a scrollable surface, and its `scroll` event does not
    /// compose out of the shadow root, so the global `document` handler never sees it - the
    /// constructor's capture-phase `this.shadowRoot` listener must. Build a real shadow root, put the
    /// rendered cell inside it, then fire `scroll` from the inner `.cell-content` scroller so the
    /// event actually routes through the capture phase (not the handler called directly). This fails
    /// if the shadow-root listener is deleted or loses its capture phase.
    {
        const page = await bootPage(js);
        const res = JSON.parse(page.run(`
          (function () {
              const qr = new QueryResultElement();
              const td = qr._renderCell('c', ${JSON.stringify(url)});
              qr.shadowRoot.appendChild(td);          // attach into the shadow tree so scroll routes to it
              function find(n, pred) {
                  if (!n) return null;
                  if (pred(n)) return n;
                  for (const ch of (n.children || [])) { const r = find(ch, pred); if (r) return r; }
                  return null;
              }
              const scroller = find(td, n => typeof n.className === 'string' && n.className.split(/\\s+/).includes('cell-content'));
              const link = find(td, n => n.tagName === 'A');
              setImagePreviewModifierHeld(true);
              link.dispatchEvent({ type: 'mouseenter', clientX: 5, clientY: 5 });
              if (image_preview && typeof image_preview.onload === 'function') image_preview.onload();
              const before = image_preview && { src: image_preview.src, display: image_preview.style.display };
              scroller.dispatchEvent({ type: 'scroll' });   // capture phase must reach the shadow root
              const after = image_preview && { src: image_preview.src, display: image_preview.style.display };
              return JSON.stringify({ hasScroller: !!scroller, hasLink: !!link, before, after });
          })()`));
        check('shadow-scroll', 'the rendered cell has an inner .cell-content scroller in the shadow root',
            res.hasScroller && res.hasLink, res);
        check('shadow-scroll', 'the preview is shown before the inner scroll',
            res.before && res.before.display === 'block' && res.before.src === url, res);
        check('shadow-scroll', 'scrolling the inner shadow-tree scroller dismisses the preview',
            res.after && res.after.display === 'none' && res.after.src === '', res);
    }

    /// Contract 4: pressing the modifier while the cursor already rests on a link must not fetch by
    /// itself (a bare `keydown` such as `Ctrl+Enter` over an image link is not an opt-in); the fetch
    /// happens only on the next pointer movement over the link.
    {
        const h = await boot(js, url);
        h.pointer('mouseenter');            // hover first, no modifier -> no fetch
        h.keydown('Control');               // arm the gate while hovering
        check('keydown-not-a-fetch', 'arming the modifier while hovering does not fetch',
            h.state() === null, h.state());
        h.pointer('mousemove', 40, 40);     // now move -> fetch
        const s = h.state();
        check('keydown-not-a-fetch', 'the following pointer move fetches once',
            s && s.src === url && s.assignments === 1, s);
    }

    /// Contract 5: teardown is anchored to the ACTIVE result lifecycle, not the auxiliary global
    /// `clear` wrapper. With the tabs rework, a rerun tears the result down through `clearPanel`
    /// (called from `postSingle`/`postMulti`/`materializeResult`/`beginFlight`), a tab switch hides
    /// the panel through `setActivePanel`, and closing a tab removes the panel from the DOM. Each of
    /// these production paths must dismiss a preview owned by the affected result, WITHOUT a
    /// background/unrelated teardown dismissing a preview owned by another result. These scenarios run
    /// the real `clearPanel`/`setActivePanel`/`panel.remove()` against genuine `QueryResultElement`s.

    /// 5a: a rerun through the production `clearPanel` dismisses the owning tab's preview.
    {
        const { run } = await bootRealTab(js, url);
        let s = ownedState(run);
        check('clearPanel-rerun', 'preview is shown and owned before the rerun',
            s && s.display === 'block' && s.src === url && s.owner === 'set', s);
        run(`clearPanel(getActiveTab());`);
        s = ownedState(run);
        check('clearPanel-rerun', 'rerun through clearPanel dismisses the preview',
            s && s.display === 'none' && s.src === '' && s.owner === null, s);
    }

    /// 5b: clearing a BACKGROUND tab must not dismiss a preview owned by the active tab; clearing the
    /// owning (active) tab then does dismiss it.
    {
        const { run } = await bootRealTab(js, url);
        installBackgroundTab(run);
        let s = ownedState(run);
        check('background-clear', 'preview is shown before the background clear', s && s.display === 'block', s);
        run(`clearPanel(tabs.find(t => t.id === __bgId));`);
        s = ownedState(run);
        check('background-clear', 'clearing a background tab does not dismiss the active preview',
            s && s.display === 'block' && s.src === url && s.owner === 'set', s);
        run(`clearPanel(getActiveTab());`);
        s = ownedState(run);
        check('background-clear', 'clearing the owning (active) tab dismisses it',
            s && s.display === 'none' && s.owner === null, s);
    }

    /// 5c: switching tabs (production `setActivePanel`) dismisses a preview owned by the tab left
    /// behind (its link is hidden with the panel, no `mouseleave`), but re-activating the owning tab
    /// keeps it.
    {
        const { run } = await bootRealTab(js, url);
        installBackgroundTab(run);
        run(`setActivePanel(getActiveTab());`);   // re-activate the owner: preview must stay
        let s = ownedState(run);
        check('panel-switch', 're-activating the owning tab keeps the preview', s && s.display === 'block', s);
        run(`setActivePanel(tabs.find(t => t.id === __bgId));`);
        s = ownedState(run);
        check('panel-switch', 'switching to another tab dismisses a preview owned by the tab left behind',
            s && s.display === 'none' && s.src === '' && s.owner === null, s);
    }

    /// 5d: closing a tab removes its panel from the DOM (production does `closing.panel.remove()`),
    /// whose `disconnectedCallback` dismisses the preview it owned; removing a background tab does not.
    {
        const { run } = await bootRealTab(js, url);
        let s = ownedState(run);
        check('tab-removal', 'preview is shown before removing the owning tab', s && s.display === 'block', s);
        run(`getActiveTab().panel.remove();`);
        s = ownedState(run);
        check('tab-removal', 'removing the owning tab (panel.remove -> disconnectedCallback) dismisses the preview',
            s && s.display === 'none' && s.src === '' && s.owner === null, s);
    }
    {
        const { run } = await bootRealTab(js, url);
        installBackgroundTab(run);
        run(`tabs.find(t => t.id === __bgId).panel.remove();`);
        const s = ownedState(run);
        check('tab-removal', 'removing a background tab does not dismiss the active preview',
            s && s.display === 'block' && s.owner === 'set', s);
    }

    if (failures === 0) {
        console.log('All scenarios passed');
        process.exit(0);
    } else {
        console.log(`${failures} check(s) failed`);
        process.exit(1);
    }
}

main().catch((e) => { console.error(e && e.stack || e); process.exit(1); });
