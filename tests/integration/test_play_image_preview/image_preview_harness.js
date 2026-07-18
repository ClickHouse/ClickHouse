#!/usr/bin/env node
/// Executable regression harness for the Web UI image-preview hover gesture (`attachImagePreview`,
/// `showImagePreview`, `hideImagePreview`, `setImagePreviewModifierHeld`).
///
/// The preview fetches a URL taken from untrusted query results, so its contract is
/// security-sensitive:
///   * plain hover (no modifier) must NEVER assign `src`, i.e. must never issue an outbound request;
///   * only `Ctrl` (or `Cmd` on Mac) + hover may assign `src`;
///   * `scroll`, `clear`, `blur` and modifier-release must tear the preview down - clear `src` and
///     suppress a load that finishes after the preview was dismissed.
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

function makeElement(tag) {
    const listeners = new Map();
    const attributes = new Map();
    /// A real `<img>` reflects the `src` attribute in the `src` property and aborts the load when
    /// `src` is removed. Track every non-empty assignment (each is one outbound request) so a
    /// scenario can assert whether hovering fetched, and clear the property on `removeAttribute`.
    let src_value = '';
    const src_assignments = [];
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
        alt: '',
        hidden: false,
        /// An <img> that has not finished loading: keeps `showImagePreview` on the fetch-then-reveal
        /// path rather than the already-loaded fast path until a scenario marks it complete.
        complete: false,
        naturalWidth: 0,
        onload: null,
        onerror: null,
        /// The number of times `src` was assigned a non-empty URL, i.e. outbound requests issued.
        src_assignments,

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
        setAttribute(k, v) {
            attributes.set(k, String(v));
            if (k === 'id') el.id = String(v);
            if (k === 'src') { src_value = String(v); if (src_value) src_assignments.push(src_value); }
        },
        getAttribute(k) { return attributes.has(k) ? attributes.get(k) : null; },
        removeAttribute(k) {
            attributes.delete(k);
            /// Removing `src` aborts the in-flight load and empties the property, exactly as a
            /// browser does; this is what `hideImagePreview` relies on to stop a background fetch.
            if (k === 'src') { src_value = ''; }
        },
        hasAttribute(k) { return attributes.has(k); },
        focus() {},
        blur() {},
        click() { el.dispatchEvent({ type: 'click' }); },
        select() {},
        setSelectionRange() {},
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
        /// Inert versions of the custom-element methods the page script calls (the custom-element
        /// upgrade never runs in this stub DOM).
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

/// Boot a fresh page and install a single image link that would preview `url`, exactly as the
/// result renderer does (`attachImagePreview`). Returns helpers to drive the gesture and read state.
async function boot(js, url) {
    const { sandbox, fetch_count } = makeContext();
    vm.runInContext(js, sandbox, { filename: 'play.html.js' });
    /// The image-preview functions and window listeners are defined synchronously at top level; let
    /// the async startup (IndexedDB open, reconciliation) settle so nothing races the scenario.
    await sleep(20);

    /// Build the link the same way the result renderer does, then wire the preview onto it.
    vm.runInContext(
        `globalThis.__link = document.createElement('a');
         globalThis.__link.href = ${JSON.stringify(url)};
         attachImagePreview(__link, ${JSON.stringify(url)});`,
        sandbox);

    const run = (code) => vm.runInContext(code, sandbox);

    return {
        sandbox,
        fetchCount: fetch_count,
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

    if (failures === 0) {
        console.log('All scenarios passed');
        process.exit(0);
    } else {
        console.log(`${failures} check(s) failed`);
        process.exit(1);
    }
}

main().catch((e) => { console.error(e && e.stack || e); process.exit(1); });
