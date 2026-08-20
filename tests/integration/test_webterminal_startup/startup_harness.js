#!/usr/bin/env node
/// Executable regression harness for the `/webterminal` startup and authentication flow.
///
/// Runs the REAL script extracted from the served `webterminal.html` inside a Node `vm` context
/// with a stubbed browser environment (a fake `xterm.js` terminal that records everything written
/// to it, a fake `WebSocket` that records every connection attempt, and a `window` whose `parent`
/// is a foreign object when the page is meant to look embedded). Each scenario then drives the
/// page the way a browser would - the host page posts credentials, the user types, the server
/// accepts or rejects the authentication - and asserts which connections were opened and what the
/// terminal displayed.
///
/// The main contract: a page embedded without a `user` URL parameter must not connect on its own.
/// The host page owns the credentials, so a connection opened before they arrive can only
/// authenticate as a passwordless user and is immediately replaced by the credentialed one,
/// which used to print `Connecting...` twice.
///
/// Driven by `test.py` inside the `clickhouse/mysql-js-client` container (node:22-alpine),
/// against the `/webterminal` page served by a real ClickHouse server. Can also be run standalone
/// against a checkout for development: node startup_harness.js programs/server/webterminal.html
///
/// Usage: node startup_harness.js <path-or-url-of-webterminal.html>
/// Exit code 0 = all scenarios pass; 1 = failure (details on stdout).

'use strict';

const vm = require('vm');
const fs = require('fs');

/// ----- Fake browser environment -------------------------------------------------------------

/// The page is served by the same server it connects to, so one host is enough.
const PAGE_ORIGIN = 'http://127.0.0.1:8123';
/// A host page origin the page must trust (see `isTrustedParentOrigin`).
const HOST_ORIGIN = 'https://console.clickhouse.cloud';

function makeContext({ embedded, search }) {
    /// Everything the page writes to the terminal, in order.
    const writes = [];
    /// Every `WebSocket` the page created, in order. Nothing here is connected implicitly:
    /// a scenario decides when a connection opens (`accept`) or is refused (`reject`).
    const sockets = [];
    let dataHandler = null;

    class FakeWebSocket {
        static CONNECTING = 0;
        static OPEN = 1;
        static CLOSING = 2;
        static CLOSED = 3;

        constructor(url) {
            this.url = url;
            this.readyState = FakeWebSocket.CONNECTING;
            this.sent = [];
            this.closedByPage = false;
            sockets.push(this);
        }
        send(data) { this.sent.push(data); }
        close() {
            this.closedByPage = true;
            this.readyState = FakeWebSocket.CLOSED;
        }
        /// The handshake succeeded: the page sends its authentication message from `onopen`.
        accept() {
            this.readyState = FakeWebSocket.OPEN;
            if (this.onopen) this.onopen();
        }
        /// The server refused the authentication (close code 1008) or the handshake failed.
        reject(code) {
            this.readyState = FakeWebSocket.CLOSED;
            if (this.onclose) this.onclose({ code: code === undefined ? 1008 : code });
        }
        /// The authenticated session ended normally.
        hangUp() {
            this.readyState = FakeWebSocket.CLOSED;
            if (this.onclose) this.onclose({ code: 1000 });
        }
        /// The authentication message the page sent as the first frame, if any.
        auth() {
            const first = this.sent[0];
            return typeof first === 'string' ? JSON.parse(first) : null;
        }
    }

    const term = {
        cols: 80,
        rows: 24,
        write(data) { writes.push(String(data)); },
        open() {},
        focus() {},
        loadAddon() {},
        attachCustomKeyEventHandler() {},
        onData(callback) { dataHandler = callback; },
        onResize() {},
    };

    const listeners = new Map();
    const window = {
        location: {
            protocol: 'http:',
            host: '127.0.0.1:8123',
            origin: PAGE_ORIGIN,
            search: search || '',
        },
        addEventListener(type, callback) {
            if (!listeners.has(type)) listeners.set(type, []);
            listeners.get(type).push(callback);
        },
        postMessage() {},
    };
    window.window = window;
    window.self = window;
    /// `window.parent !== window` is how the page detects that it is embedded.
    window.parent = embedded ? { postMessage() {} } : window;

    const sandbox = window;
    sandbox.document = { getElementById: () => ({ clientWidth: 800, clientHeight: 400 }) };
    sandbox.Terminal = function() { return term; };
    sandbox.FitAddon = { FitAddon: function() { return { fit() {} }; } };
    sandbox.WebLinksAddon = { WebLinksAddon: function() { return {}; } };
    sandbox.WebSocket = FakeWebSocket;
    sandbox.URL = URL;
    sandbox.URLSearchParams = URLSearchParams;
    sandbox.TextEncoder = TextEncoder;
    sandbox.requestAnimationFrame = (callback) => callback();
    sandbox.console = console;
    vm.createContext(sandbox);

    return {
        sandbox,
        writes,
        sockets,
        /// Everything displayed so far, including the ANSI control sequences.
        text: () => writes.join(''),
        /// How many times the page announced a connection attempt to the user.
        connectingCount: () => writes.filter((w) => w.includes('Connecting...')).length,
        /// The page's own state machine ('connecting', 'waiting_credentials', 'prompt_user', ...).
        state: () => vm.runInContext('state', sandbox),
        /// The user types into the terminal.
        type: (data) => dataHandler(data),
        /// A `postMessage` from the embedding host page.
        post: (data, options) => {
            const opts = options || {};
            const event = {
                source: 'source' in opts ? opts.source : window.parent,
                origin: 'origin' in opts ? opts.origin : HOST_ORIGIN,
                data,
            };
            for (const callback of listeners.get('message') || [])
                callback(event);
        },
    };
}

function extractScript(html) {
    const blocks = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)].map((m) => m[1]);
    if (!blocks.length) throw new Error('no <script> block found in webterminal.html');
    return blocks.reduce((a, b) => (a.length >= b.length ? a : b));
}

/// Load the page: this runs the whole top-level script, including its startup decision.
function loadPage(js, config) {
    const env = makeContext(config);
    vm.runInContext(js, env.sandbox, { filename: 'webterminal.html.js' });
    return env;
}

/// ----- Assertions ---------------------------------------------------------------------------

let failures = 0;

function check(scenario, what, cond, actual) {
    if (cond) {
        console.log(`PASS [${scenario}] ${what}`);
    } else {
        failures++;
        console.log(`FAIL [${scenario}] ${what} -- actual: ${JSON.stringify(actual)}`);
    }
}

function sendCredentials(env, user, password, options) {
    env.post({ type: 'webterminal-credentials', user, password }, options);
}

function main() {
    const src = process.argv[2];
    if (!src) {
        console.error('usage: node startup_harness.js <path-or-url-of-webterminal.html>');
        process.exit(2);
    }
    let html;
    if (/^https?:/.test(src)) {
        /// `fetch` is available in the Node version shipped by the client image.
        return fetch(src).then((resp) => {
            if (!resp.ok) throw new Error(`GET ${src} -> HTTP ${resp.status}`);
            return resp.text();
        }).then(run);
    }
    html = fs.readFileSync(src, 'utf8');
    return Promise.resolve(run(html));

    function run(pageHtml) {
        const js = extractScript(pageHtml);
        runScenarios(js);
        if (failures) {
            console.log(`${failures} check(s) failed`);
            process.exit(1);
        }
        console.log('All scenarios passed');
    }
}

function runScenarios(js) {
    /// Contract 1: opened as a normal page, the terminal connects by itself, exactly once, and
    /// falls back to the interactive prompt when the server refuses the empty password.
    {
        const s = 'standalone';
        const env = loadPage(js, { embedded: false, search: '' });
        check(s, 'connects at startup', env.sockets.length === 1, env.sockets.length);
        check(s, 'announces the attempt once', env.connectingCount() === 1, env.connectingCount());
        check(s, 'connects to the /webterminal endpoint',
            env.sockets[0].url === 'ws://127.0.0.1:8123/webterminal', env.sockets[0].url);
        env.sockets[0].reject(1008);
        check(s, 'asks for a username after the rejection', env.text().includes('Username: '), env.text());
        check(s, 'is in the username prompt state', env.state() === 'prompt_user', env.state());
    }

    /// Contract 2: embedded without a `user` URL parameter, the terminal waits for the host page
    /// and then connects exactly once, with the credentials the host page supplied. A connection
    /// opened before the credentials arrive - and the second `Connecting...` it printed - is the
    /// regression this scenario pins down.
    {
        const s = 'embedded-credentials';
        const env = loadPage(js, { embedded: true, search: '' });
        check(s, 'does not connect before the credentials arrive', env.sockets.length === 0, env.sockets.length);
        check(s, 'waits for the credentials', env.state() === 'waiting_credentials', env.state());
        check(s, 'says so', env.text().includes('Waiting for credentials'), env.text());
        check(s, 'announces no connection attempt yet', env.connectingCount() === 0, env.connectingCount());

        sendCredentials(env, 'alice', 'secret');
        check(s, 'connects once the credentials arrive', env.sockets.length === 1, env.sockets.length);
        check(s, 'announces the attempt exactly once', env.connectingCount() === 1, env.connectingCount());

        env.sockets[0].accept();
        check(s, 'authenticates with the host page credentials',
            JSON.stringify(env.sockets[0].auth()) === JSON.stringify({ type: 'auth', password: 'secret', user: 'alice' }),
            env.sockets[0].auth());
        check(s, 'runs the session', env.state() === 'active', env.state());
    }

    /// Contract 3: an empty user from the host page means "the server default user", which is not
    /// necessarily named `default`, so the `user` field is omitted from the authentication message.
    {
        const s = 'embedded-default-user';
        const env = loadPage(js, { embedded: true, search: '' });
        sendCredentials(env, '', '');
        check(s, 'connects once', env.sockets.length === 1, env.sockets.length);
        env.sockets[0].accept();
        check(s, 'omits the user so the server picks its default',
            env.sockets[0].auth() !== null && !('user' in env.sockets[0].auth()), env.sockets[0].auth());
    }

    /// Contract 4: credentials from the host page are explicit, so a rejection must reach the
    /// user as a prompt instead of silently waiting for another (one-shot) host page message.
    {
        const s = 'embedded-rejected';
        const env = loadPage(js, { embedded: true, search: '' });
        sendCredentials(env, 'alice', 'wrong');
        env.sockets[0].reject(1008);
        check(s, 'asks for the password of the supplied user',
            env.text().includes('Password: ') && !env.text().includes('Username: '), env.text());
        check(s, 'is in the password prompt state', env.state() === 'prompt_password', env.state());
        env.type('right\r');
        check(s, 'retries with the typed password', env.sockets.length === 2, env.sockets.length);
        env.sockets[1].accept();
        check(s, 'keeps the user and takes the new password',
            JSON.stringify(env.sockets[1].auth()) === JSON.stringify({ type: 'auth', password: 'right', user: 'alice' }),
            env.sockets[1].auth());
    }

    /// Contract 5: a host page that never sends credentials (it does not implement the protocol,
    /// or its origin is not trusted) must not leave a dead terminal: Enter hands the login over
    /// to the user, and credentials arriving afterwards must not hijack the interactive login.
    {
        const s = 'embedded-idle-takeover';
        const env = loadPage(js, { embedded: true, search: '' });
        env.type('x');
        check(s, 'a stray key does not end the wait', env.state() === 'waiting_credentials', env.state());
        check(s, 'a stray key opens no connection', env.sockets.length === 0, env.sockets.length);

        env.type('\r');
        check(s, 'Enter asks for a username', env.text().includes('Username: '), env.text());
        check(s, 'Enter opens no connection by itself', env.sockets.length === 0, env.sockets.length);

        sendCredentials(env, 'alice', 'secret');
        check(s, 'late credentials do not hijack the login',
            env.sockets.length === 0 && env.state() === 'prompt_user', env.state());

        env.type('bob\r');
        env.type('pw\r');
        check(s, 'the interactive login connects', env.sockets.length === 1, env.sockets.length);
        env.sockets[0].accept();
        check(s, 'uses the typed credentials',
            JSON.stringify(env.sockets[0].auth()) === JSON.stringify({ type: 'auth', password: 'pw', user: 'bob' }),
            env.sockets[0].auth());
    }

    /// Contract 6: a `user` URL parameter takes over the credential handling, embedded or not:
    /// the terminal connects immediately and asks for that user's password when refused.
    {
        const s = 'embedded-user-in-url';
        const env = loadPage(js, { embedded: true, search: '?user=carol' });
        check(s, 'connects at startup', env.sockets.length === 1, env.sockets.length);
        check(s, 'announces the attempt once', env.connectingCount() === 1, env.connectingCount());
        env.sockets[0].accept();
        check(s, 'tries the URL user with an empty password',
            JSON.stringify(env.sockets[0].auth()) === JSON.stringify({ type: 'auth', password: '', user: 'carol' }),
            env.sockets[0].auth());

        const refused = loadPage(js, { embedded: true, search: '?user=carol' });
        refused.sockets[0].reject(1008);
        check(s, 'asks only for the password after the rejection',
            refused.text().includes('Password: ') && !refused.text().includes('Username: '), refused.text());
    }

    /// Contract 7: while waiting, the page must accept credentials only from the embedding window
    /// and only from a trusted origin - the wait state is now the single way into an embedded
    /// session, so a hole in either check would hand the terminal to a foreign page.
    {
        const s = 'untrusted-credentials';
        const foreign = loadPage(js, { embedded: true, search: '' });
        sendCredentials(foreign, 'alice', 'secret', { origin: 'https://evil.example.com' });
        check(s, 'ignores credentials from an untrusted origin',
            foreign.sockets.length === 0 && foreign.state() === 'waiting_credentials', foreign.state());

        const insecure = loadPage(js, { embedded: true, search: '' });
        sendCredentials(insecure, 'alice', 'secret', { origin: 'http://console.clickhouse.cloud' });
        check(s, 'ignores credentials from a trusted host over plain HTTP',
            insecure.sockets.length === 0 && insecure.state() === 'waiting_credentials', insecure.state());

        const other = loadPage(js, { embedded: true, search: '' });
        sendCredentials(other, 'alice', 'secret', { source: { postMessage() {} } });
        check(s, 'ignores credentials from a window that is not the parent',
            other.sockets.length === 0 && other.state() === 'waiting_credentials', other.state());

        const parent = loadPage(js, { embedded: true, search: '' });
        sendCredentials(parent, 'alice', 'secret', { origin: PAGE_ORIGIN });
        check(s, 'accepts credentials from a same-origin parent', parent.sockets.length === 1, parent.sockets.length);
    }
}

Promise.resolve()
    .then(main)
    .catch((e) => {
        console.log(`FAIL harness error: ${e && e.stack ? e.stack : e}`);
        process.exit(1);
    });
