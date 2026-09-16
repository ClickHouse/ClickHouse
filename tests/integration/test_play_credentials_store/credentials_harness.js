#!/usr/bin/env node
/// Executable regression harness for the Web UI's password-manager round trip: `storeCredentials`
/// (what name a login is remembered under), `effectiveConnectionUser` (the connection identity) and
/// `userUrlParam` (the `user` parameter of every request URL).
///
/// The contract under test: an empty `user` field authenticates implicitly - as the server's
/// `default` user, or as the user embedded in the server URL's userinfo (`http://alice@host:8123/`).
/// The login is remembered under that effective name, so the password manager refills it into the
/// field on the next visit. The refilled name must NOT change the connection: the request URL built
/// for a field holding the implicit user must be byte-identical to the one built for an empty field
/// (no forced `user=default` that would override the URL userinfo), and the connection identity used
/// by the history / database-panel gates must compare equal. A field naming a DIFFERENT user still
/// takes precedence over the userinfo, as before.
///
/// So that the suite proves the production wiring rather than a re-statement of it, the scenarios
/// run the REAL functions extracted from the served `play.html`: `storeCredentials` against a fake
/// `PasswordCredential` / `navigator.credentials`, and the real request builders (`getServerStatus`
/// with a stubbed `fetch`, `buildCompletionUrl`) for the wire URL. A source-level check pins that no
/// request builder appends `&user=` on its own, bypassing `userUrlParam`.
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
        ctx.fetched.push({ url, options });
        return { ok: true, json: async () => ({ v: 'test', t: 0 }) };
    };
    vm.createContext(ctx);
    return ctx;
}

const FUNCTIONS = ['effectiveConnectionUser', 'userUrlParam', 'storeCredentials', 'getServerStatus', 'buildCompletionUrl'];

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
    const status_url = ctx.fetched[0].url;
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

/// Run once with an empty field, then simulate the password manager refilling the remembered
/// login, and require the refilled state to be the same connection on the wire and in identity.
async function roundTrip(ctx, server, expected_id) {
    ctx.url_elem.value = server;
    ctx.user_elem.value = '';
    ctx.password_elem.value = 'secret';
    const before = await requestUrls(ctx);
    const identity_before = connectionIdentity(ctx);
    vm.runInContext('storeCredentials()', ctx);
    assertEqual(ctx.stored.length, 1, 'one credential stored');
    assertEqual(ctx.stored[0].id, expected_id, 'remembered id');
    assertEqual(ctx.stored[0].password, 'secret', 'remembered password');
    assertEqual(ctx.stored[0].name, server, 'remembered name is the server URL');
    if (!before.status_url.includes('user=') && !before.completion_url.includes('user=')) {
        /// The implicit-auth request carries no `user` parameter at all.
    } else {
        throw new Error(`the empty-field request unexpectedly carries a user parameter: ${before.status_url}`);
    }

    /// The password manager refills the remembered login into the fields on the next visit.
    ctx.user_elem.value = ctx.stored[0].id;
    ctx.password_elem.value = ctx.stored[0].password;
    const after = await requestUrls(ctx);
    assertEqual(after.status_url, before.status_url, 'status request URL after refill');
    assertEqual(after.completion_url, before.completion_url, 'completion request URL after refill');
    assertEqual(connectionIdentity(ctx), identity_before, 'connection identity after refill');
    if (after.status_url.includes('user=')) throw new Error(`refilled implicit login forced a user parameter: ${after.status_url}`);
}

scenario('implicit-default-round-trip', async js => {
    const ctx = boot(js);
    await roundTrip(ctx, 'http://host:8123/', 'default');
});

scenario('implicit-default-round-trip-with-query-string', async js => {
    const ctx = boot(js);
    await roundTrip(ctx, 'http://host:8123/?framing_output_format=None', 'default');
});

scenario('userinfo-round-trip', async js => {
    const ctx = boot(js);
    await roundTrip(ctx, 'http://alice@host:8123/', 'alice');
});

scenario('percent-encoded-userinfo-round-trip', async js => {
    const ctx = boot(js);
    await roundTrip(ctx, 'http://a%40corp@host:8123/', 'a@corp');
});

/// A refilled `default` on a userinfo connection is NOT the implicit login of that connection: it
/// must keep overriding the userinfo, exactly as a typed `default` did before.
scenario('explicit-default-overrides-userinfo', async js => {
    const ctx = boot(js);
    ctx.url_elem.value = 'http://alice@host:8123/';
    ctx.user_elem.value = 'default';
    ctx.password_elem.value = 'secret';
    const { status_url, completion_url } = await requestUrls(ctx);
    if (!status_url.includes('&user=default&')) throw new Error(`explicit default not sent: ${status_url}`);
    if (!completion_url.includes('&user=default&')) throw new Error(`explicit default not sent: ${completion_url}`);
    assertEqual(connectionIdentity(ctx), 'default', 'field takes precedence over userinfo');
    vm.runInContext('storeCredentials()', ctx);
    assertEqual(ctx.stored[0].id, 'default', 'remembered under the explicit name');
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
    vm.runInContext('storeCredentials()', ctx);
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
    vm.runInContext('storeCredentials()', ctx);
    assertEqual(ctx.stored.length, 0, 'nothing stored without a password');
});

/// Firefox has no `PasswordCredential` at all: the call must be skipped, not throw a `ReferenceError`.
scenario('no-password-credential-api-skips', async js => {
    const ctx = boot(js, { withPasswordCredential: false });
    ctx.url_elem.value = 'http://host:8123/';
    ctx.user_elem.value = '';
    ctx.password_elem.value = 'secret';
    vm.runInContext('storeCredentials()', ctx);
    assertEqual(ctx.stored.length, 0, 'nothing stored without the API');
});

scenario('malformed-server-url-still-stores-default', async js => {
    const ctx = boot(js);
    ctx.url_elem.value = 'http://[bad';
    ctx.user_elem.value = '';
    ctx.password_elem.value = 'secret';
    assertEqual(connectionIdentity(ctx), 'default', 'identity of an unparsable server URL');
    vm.runInContext('storeCredentials()', ctx);
    assertEqual(ctx.stored.length, 1, 'stored');
    assertEqual(ctx.stored[0].id, 'default', 'remembered id');
});

/// Every request URL of the page must obtain its `user` parameter from `userUrlParam`; a builder
/// appending `&user=` by hand would reintroduce the forced `user=default` after a refill.
scenario('no-request-builder-bypasses-userUrlParam', async js => {
    const occurrences = [...js.matchAll(/&user=/g)].length;
    const inside_helper = [...extractFunction(js, 'userUrlParam').matchAll(/&user=/g)].length;
    assertEqual(inside_helper, 1, '`userUrlParam` appends the parameter once');
    assertEqual(occurrences, inside_helper, 'occurrences of `&user=` outside `userUrlParam`');
    const users = [...js.matchAll(/userUrlParam\(server_address, user\)/g)].length;
    if (users < 5) throw new Error(`expected at least 5 request builders to call userUrlParam, found ${users}`);
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
