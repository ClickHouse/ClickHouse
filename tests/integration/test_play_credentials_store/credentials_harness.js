#!/usr/bin/env node
/// Executable regression harness for the Web UI's password-manager round trip: `storeCredentials`
/// (what name a login is remembered under) and the request URLs built for the remembered login.
///
/// The contract under test: an empty `user` field makes the server authenticate the request as its
/// `default_session_user`, which is server configuration (not necessarily `default`). The login is
/// remembered under the name the server reports as `currentUser` for a request without a `user`
/// parameter (never a hard-coded `default`), so the password manager refills it into the field on the
/// next visit and the refilled name selects the same account explicitly. Nothing is stored if the
/// server cannot tell - including for a server URL with a userinfo (`http://alice@host:8123/`), which
/// `fetch` refuses to send. The probe is sent outside of the connection's HTTP session, because it
/// runs concurrently with the query of the Run and the server rejects concurrent requests in one
/// session with `SESSION_IS_LOCKED`.
///
/// So that the suite proves the production wiring rather than a re-statement of it, the scenarios
/// run the REAL functions extracted from the served `play.html`: `storeCredentials` against a fake
/// `PasswordCredential` / `navigator.credentials`, and the real request builders (`getServerStatus`
/// with a stubbed `fetch` that, like the browser, rejects URLs with credentials, and
/// `buildCompletionUrl`) for the wire URL.
///
/// The stateless suite has no JavaScript runtime, so the contract is driven by this Node.js harness
/// executed inside the `clickhouse/mysql-js-client` container (node:22-alpine) against the `/play`
/// page served by a real ClickHouse server. Can also be run standalone against a checkout:
///   node credentials_harness.js programs/server/play.html
///
/// Usage: node credentials_harness.js <path-or-url-of-play.html>
/// Exit code 0 = all scenarios pass; 1 = failure (details on stdout).

'use strict';

const vm = require('vm');
const fs = require('fs');

async function loadHtml(source) {
    if (/^https?:\/\//.test(source)) {
        const response = await fetch(source);
        if (!response.ok) throw new Error(`GET ${source} -> ${response.status}`);
        return await response.text();
    }
    return fs.readFileSync(source, 'utf8');
}

function extractScript(html) {
    const blocks = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)].map(m => m[1]);
    if (!blocks.length) throw new Error('no <script> block found in play.html');
    return blocks.join('\n');
}

/// The source of a top-level `function NAME(` / `async function NAME(` declaration, up to its
/// matching closing brace (string literals and comments are skipped while matching braces).
function extractFunction(js, name) {
    const m = js.match(new RegExp(`(?:^|\\n)(async\\s+)?function\\s+${name}\\s*\\(`));
    if (!m) throw new Error(`function ${name} not found in the page script`);
    const start = m.index + (m[0].startsWith('\n') ? 1 : 0);
    let i = js.indexOf('{', start);
    let depth = 0;
    for (; i < js.length; ++i) {
        const c = js[i];
        if (c === '/' && js[i + 1] === '/') { i = js.indexOf('\n', i); continue; }
        if (c === '/' && js[i + 1] === '*') { i = js.indexOf('*/', i) + 1; continue; }
        if (c === '\'' || c === '"' || c === '`') {
            for (++i; i < js.length && js[i] !== c; ++i) if (js[i] === '\\') ++i;
            continue;
        }
        if (c === '{') ++depth;
        if (c === '}' && --depth === 0) return js.slice(start, i + 1);
    }
    throw new Error(`unbalanced braces in function ${name}`);
}

/// ----- Fake browser --------------------------------------------------------------

function makeContext() {
    const ctx = {
        url_elem: { value: '' },
        user_elem: { value: '' },
        password_elem: { value: '' },
        location: { href: 'http://localhost:8123/play', origin: 'http://localhost:8123' },
        stored: [],
        fetched: [],
        json: undefined,
        /// What the fake server reports as `currentUser` for a request without a `user` parameter
        /// (its `default_session_user`); `null` makes the status request fail.
        implicit_user: 'default',
        URL,
        console,
        encodeURIComponent,
        decodeURIComponent,
    };
    ctx.PasswordCredential = class PasswordCredential {
        constructor({ id, password, name }) {
            /// Mirrors the browser: an empty `id` or `password` is a `TypeError`.
            if (!id) throw new TypeError("Failed to construct 'PasswordCredential': 'id' must not be empty.");
            if (!password) throw new TypeError("Failed to construct 'PasswordCredential': 'password' must not be empty.");
            this.id = id;
            this.password = password;
            this.name = name;
        }
    };
    ctx.navigator = { credentials: { store(cred) { ctx.stored.push(cred); return Promise.resolve(cred); } } };
    ctx.fetch = async (url, options) => {
        /// Mirrors the browser: `fetch` refuses a URL with credentials before sending anything.
        if (new URL(url).username) throw new TypeError(`Request cannot be constructed from a URL that includes credentials: ${url}`);
        ctx.fetched.push({ url, options });
        if (ctx.implicit_user === null) return { ok: false, json: async () => ({}) };
        const m = url.match(/[?&]user=([^&]*)/);
        const u = m ? decodeURIComponent(m[1]) : ctx.implicit_user;
        return { ok: true, json: async () => ({ v: 'test', t: 0, u }) };
    };
    vm.createContext(ctx);
    return ctx;
}

const FUNCTIONS = ['effectiveConnectionUser', 'serverAddressWithoutSession', 'storeCredentials', 'getServerStatus', 'buildCompletionUrl'];

function boot(js, { withPasswordCredential = true } = {}) {
    const ctx = makeContext();
    if (!withPasswordCredential) delete ctx.PasswordCredential;
    vm.runInContext(FUNCTIONS.map(name => extractFunction(js, name)).join('\n\n'), ctx);
    return ctx;
}

/// The wire URL of the requests the page issues for the current (url, user, password) fields.
async function requestUrls(ctx) {
    ctx.fetched.length = 0;
    await vm.runInContext('getServerStatus(url_elem.value, user_elem.value, password_elem.value)', ctx);
    const status_url = ctx.fetched.length ? ctx.fetched[0].url : null;
    const completion_url = vm.runInContext('buildCompletionUrl()', ctx);
    return { status_url, completion_url };
}

function connectionIdentity(ctx) {
    return vm.runInContext('effectiveConnectionUser(url_elem.value, user_elem.value)', ctx);
}

/// ----- Scenarios ------------------------------------------------------------------

const scenarios = [];
function scenario(name, fn) { scenarios.push({ name, fn }); }

function assertEqual(actual, expected, what) {
    if (actual !== expected) throw new Error(`${what}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
}

async function store(ctx) {
    ctx.fetched.length = 0;
    await vm.runInContext('storeCredentials()', ctx);
}

/// An empty field authenticates as the server's `default_session_user`. The login
/// must be remembered under the name the server reports for exactly that request (no `user`
/// parameter), and the refilled name must then select that account explicitly.
async function implicitRoundTrip(ctx, server, implicit_user) {
    ctx.implicit_user = implicit_user;
    ctx.url_elem.value = server;
    ctx.user_elem.value = '';
    ctx.password_elem.value = 'secret';
    const before = await requestUrls(ctx);
    if (before.status_url.includes('user=') || before.completion_url.includes('user='))
        throw new Error(`the empty-field request unexpectedly carries a user parameter: ${before.status_url}`);
    assertEqual(connectionIdentity(ctx), '', 'identity of the fully implicit connection is not guessed');
    await store(ctx);
    assertEqual(ctx.fetched.length, 1, 'the implicit user is asked from the server');
    assertEqual(ctx.fetched[0].url, before.status_url, 'the probe is the empty-field request');
    if (!ctx.fetched[0].options.body.includes('currentUser()')) throw new Error(`probe does not ask currentUser: ${ctx.fetched[0].options.body}`);
    assertEqual(ctx.stored.length, 1, 'one credential stored');
    assertEqual(ctx.stored[0].id, implicit_user, 'remembered id is the server-reported implicit user');
    assertEqual(ctx.stored[0].password, 'secret', 'remembered password');
    assertEqual(ctx.stored[0].name, server, 'remembered name is the server URL');

    ctx.user_elem.value = ctx.stored[0].id;
    const after = await requestUrls(ctx);
    const param = '&user=' + encodeURIComponent(implicit_user) + '&';
    if (!after.status_url.includes(param)) throw new Error(`refilled login does not select ${implicit_user}: ${after.status_url}`);
    if (!after.completion_url.includes(param)) throw new Error(`refilled login does not select ${implicit_user}: ${after.completion_url}`);
    const status = await vm.runInContext('getServerStatus(url_elem.value, user_elem.value, password_elem.value)', ctx);
    assertEqual(status.u, implicit_user, 'refilled login authenticates as the same user');
}

scenario('implicit-default-round-trip', async js => {
    const ctx = boot(js);
    await implicitRoundTrip(ctx, 'http://host:8123/', 'default');
});

scenario('implicit-default-round-trip-with-query-string', async js => {
    const ctx = boot(js);
    await implicitRoundTrip(ctx, 'http://host:8123/?framing_output_format=None', 'default');
});

/// `default_session_user` is not necessarily `default`: the page must not assume it.
scenario('implicit-non-default-session-user', async js => {
    const ctx = boot(js);
    await implicitRoundTrip(ctx, 'http://host:8123/', 'alice');
});

/// A typed `default` on a server whose implicit user is someone else is a different account: it must
/// be sent, not dropped as redundant.
scenario('explicit-default-sent-without-userinfo', async js => {
    const ctx = boot(js);
    ctx.implicit_user = 'alice';
    ctx.url_elem.value = 'http://host:8123/';
    ctx.user_elem.value = 'default';
    ctx.password_elem.value = 'secret';
    const { status_url, completion_url } = await requestUrls(ctx);
    if (!status_url.includes('&user=default&')) throw new Error(`explicit default not sent: ${status_url}`);
    if (!completion_url.includes('&user=default&')) throw new Error(`explicit default not sent: ${completion_url}`);
    assertEqual(connectionIdentity(ctx), 'default', 'identity is the explicit user');
    await store(ctx);
    assertEqual(ctx.fetched.length, 0, 'no probe for an explicit user');
    assertEqual(ctx.stored[0].id, 'default', 'remembered under the explicit name');
});

scenario('implicit-user-unknown-nothing-stored', async js => {
    const ctx = boot(js);
    ctx.implicit_user = null;
    ctx.url_elem.value = 'http://host:8123/';
    ctx.user_elem.value = '';
    ctx.password_elem.value = 'secret';
    await store(ctx);
    assertEqual(ctx.stored.length, 0, 'nothing stored when the server does not report the user');
});

/// `fetch` refuses a URL with credentials, so a userinfo connection cannot be used from the page, and
/// nothing is stored for it rather than a name the page cannot authenticate with.
scenario('userinfo-nothing-stored', async js => {
    const ctx = boot(js);
    ctx.url_elem.value = 'http://alice@host:8123/';
    ctx.user_elem.value = '';
    ctx.password_elem.value = 'secret';
    await store(ctx);
    assertEqual(ctx.stored.length, 0, 'nothing stored for a userinfo connection');
});

/// The probe runs concurrently with the query of the Run, so it must not join the connection's HTTP
/// session (`SESSION_IS_LOCKED`); the other parameters of the server URL are kept as they are.
scenario('probe-outside-of-session', async js => {
    const ctx = boot(js);
    ctx.url_elem.value = 'http://host:8123/?session_id=abc&session_check=1&x=a+b%20c&session_timeout=60&session_ids=keep';
    ctx.user_elem.value = '';
    ctx.password_elem.value = 'secret';
    await store(ctx);
    assertEqual(ctx.fetched.length, 1, 'one probe');
    const probe = ctx.fetched[0].url;
    if (!probe.startsWith('http://host:8123/?x=a+b%20c&session_ids=keep&'))
        throw new Error(`probe URL does not drop exactly the session parameters: ${probe}`);
    if (probe.includes('user=')) throw new Error(`probe carries a user parameter: ${probe}`);
    assertEqual(ctx.stored[0].id, 'default', 'remembered id');
    assertEqual(ctx.stored[0].name, ctx.url_elem.value, 'remembered name is the original server URL');
});

scenario('explicit-user-sent-and-remembered', async js => {
    const ctx = boot(js);
    ctx.url_elem.value = 'http://host:8123/';
    ctx.user_elem.value = 'bob';
    ctx.password_elem.value = 'secret';
    const { status_url, completion_url } = await requestUrls(ctx);
    if (!status_url.includes('&user=bob&')) throw new Error(`explicit user not sent: ${status_url}`);
    if (!completion_url.includes('&user=bob&')) throw new Error(`explicit user not sent: ${completion_url}`);
    assertEqual(connectionIdentity(ctx), 'bob', 'identity is the explicit user');
    await store(ctx);
    assertEqual(ctx.stored[0].id, 'bob', 'remembered under the explicit name');
});

scenario('explicit-user-with-special-characters', async js => {
    const ctx = boot(js);
    ctx.url_elem.value = 'http://host:8123/';
    ctx.user_elem.value = 'a b&c=d';
    ctx.password_elem.value = 'secret';
    const { status_url } = await requestUrls(ctx);
    if (!status_url.includes('&user=a%20b%26c%3Dd&')) throw new Error(`user not percent-encoded: ${status_url}`);
});

scenario('no-password-nothing-stored', async js => {
    const ctx = boot(js);
    ctx.url_elem.value = 'http://host:8123/';
    ctx.user_elem.value = '';
    ctx.password_elem.value = '';
    await store(ctx);
    assertEqual(ctx.stored.length, 0, 'nothing stored without a password');
});

/// Firefox has no `PasswordCredential` at all: the call must be skipped, not throw a `ReferenceError`.
scenario('no-password-credential-api-skips', async js => {
    const ctx = boot(js, { withPasswordCredential: false });
    ctx.url_elem.value = 'http://host:8123/';
    ctx.user_elem.value = '';
    ctx.password_elem.value = 'secret';
    await store(ctx);
    assertEqual(ctx.stored.length, 0, 'nothing stored without the API');
    assertEqual(ctx.fetched.length, 0, 'no probe without the API');
});

scenario('malformed-server-url-nothing-stored', async js => {
    const ctx = boot(js);
    ctx.url_elem.value = 'http://[bad';
    ctx.user_elem.value = '';
    ctx.password_elem.value = 'secret';
    await store(ctx);
    assertEqual(ctx.fetched.length, 0, 'no probe to an unparsable server URL');
    assertEqual(ctx.stored.length, 0, 'nothing stored');
});

/// ----- Runner ---------------------------------------------------------------------

async function main() {
    const source = process.argv[2];
    if (!source) {
        console.log('Usage: node credentials_harness.js <path-or-url-of-play.html>');
        process.exit(2);
    }
    const js = extractScript(await loadHtml(source));
    let failed = 0;
    for (const { name, fn } of scenarios) {
        try {
            await fn(js);
            console.log(`PASS [${name}]`);
        } catch (e) {
            ++failed;
            console.log(`FAIL [${name}]: ${e && e.stack || e}`);
        }
    }
    if (failed) {
        console.log(`${failed} of ${scenarios.length} scenarios failed`);
        process.exit(1);
    }
    console.log(`All scenarios passed (${scenarios.length})`);
}

main().catch(e => { console.log(e && e.stack || e); process.exit(1); });
