#!/usr/bin/env node
/// Executable regression harness for the `/play` bookkeeping of passwordless logins.
///
/// Runs the REAL helpers extracted from the served `play.html` - the block from
/// `PASSWORDLESS_LOGINS_KEY` up to the autofill detection, which needs no DOM - against a stubbed
/// `localStorage`, and asserts the contracts of `rememberAuthenticationOutcome`:
///
///  - a successful response with an empty password remembers the login as passwordless, and a
///    successful response with a real password forgets it again;
///  - a response carrying the query's own error (a syntax error, `X-ClickHouse-Exception-Code`
///    set) follows a successful authentication, so it updates the entry the same way;
///  - a rejected login (`401` / `403`) never updates the entry, but `ACCESS_DENIED` (also `403`,
///    told apart by `X-ClickHouse-Exception-Code`) follows a successful authentication and does;
///  - an error response that did not come from the server (no `X-ClickHouse-Exception-Code`, as
///    from a proxy) never remembers an empty password, but a real password still forgets it.
///
/// Driven by `test.py` inside the `clickhouse/mysql-js-client` container (node:22-alpine),
/// against the `/play` page served by a real ClickHouse server. Can also be run standalone
/// against a checkout for development: node passwordless_harness.js programs/server/play.html
///
/// Usage: node passwordless_harness.js <path-or-url-of-play.html>
/// Exit code 0 = all scenarios pass; 1 = failure (details on stdout).

'use strict';

const vm = require('vm');
const fs = require('fs');

function extractScript(html) {
    const blocks = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)].map(m => m[1]);
    if (!blocks.length) throw new Error('no <script> block found in play.html');
    return blocks.reduce((a, b) => (a.length >= b.length ? a : b));
}

/// The bookkeeping helpers are laid out together: from `PASSWORDLESS_LOGINS_KEY` up to the autofill
/// detection (`AUTOFILL_SELECTOR`), which is where the DOM begins. A refactor that moves them fails
/// here loudly rather than silently testing nothing.
function loadHelpers(js) {
    const start = js.indexOf('const PASSWORDLESS_LOGINS_KEY');
    const end = js.indexOf('const AUTOFILL_SELECTOR', start);
    if (start < 0 || end < 0) throw new Error('passwordless-login helper markers not found in the page script');
    const storage = new Map();
    const context = {
        URL,
        JSON,
        location: { href: 'http://localhost:8123/play' },
        window: {
            localStorage: {
                getItem: k => (storage.has(k) ? storage.get(k) : null),
                setItem: (k, v) => { storage.set(k, String(v)); },
            },
        },
        /// The real one resolves a user given in the URL userinfo; the scenarios do not use it.
        effectiveConnectionUser: (server_address, user) => user,
    };
    vm.runInNewContext(js.slice(start, end) +
        '\nthis.api = { rememberAuthenticationOutcome, loadPasswordlessLogins, passwordlessLoginKey };',
        context, { filename: 'passwordless_helpers.js' });
    return context.api;
}

function response(status, exception_code = null) {
    const headers = new Map();
    if (exception_code !== null) headers.set('X-ClickHouse-Exception-Code', String(exception_code));
    return {
        status,
        ok: status >= 200 && status < 300,
        headers: { get: k => (headers.has(k) ? headers.get(k) : null) },
    };
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
        console.error('usage: node passwordless_harness.js <path-or-url-of-play.html>');
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

    const server = 'http://localhost:8123/';
    const user = 'u';

    /// Contract 1: successful responses remember an empty password and forget it on a real one.
    {
        const h = loadHelpers(js);
        const remembered = () => h.loadPasswordlessLogins().has(h.passwordlessLoginKey(server, user));
        h.rememberAuthenticationOutcome(response(200), server, user, '');
        check('success', 'an empty password is remembered', remembered(), true);
        h.rememberAuthenticationOutcome(response(200), server, user, 'secret');
        check('success', 'a real password forgets it', remembered(), false);
    }

    /// Contract 2: the query's own error follows a successful authentication.
    {
        const h = loadHelpers(js);
        const remembered = () => h.loadPasswordlessLogins().has(h.passwordlessLoginKey(server, user));
        h.rememberAuthenticationOutcome(response(400, 62), server, user, '');
        check('query-error', 'a syntax error with an empty password remembers it', remembered(), true);
        h.rememberAuthenticationOutcome(response(404, 60), server, user, 'secret');
        check('query-error', 'an unknown table with a real password forgets it', remembered(), false);
    }

    /// Contract 3: a rejected login never updates the entry.
    {
        const h = loadHelpers(js);
        const remembered = () => h.loadPasswordlessLogins().has(h.passwordlessLoginKey(server, user));
        h.rememberAuthenticationOutcome(response(401, 194), server, user, '');
        check('auth-failure', '401 with an empty password is not remembered', remembered(), false);
        h.rememberAuthenticationOutcome(response(403, 516), server, user, '');
        check('auth-failure', '403 with an empty password is not remembered', remembered(), false);
        h.rememberAuthenticationOutcome(response(200), server, user, '');
        h.rememberAuthenticationOutcome(response(403, 516), server, user, 'wrong');
        check('auth-failure', '403 with a wrong password does not forget it', remembered(), true);
        h.rememberAuthenticationOutcome(response(403), server, user, 'secret');
        check('auth-failure', '403 without the exception code does not forget it', remembered(), true);
    }

    /// Contract 3a: `ACCESS_DENIED` (also `403`) follows a successful authentication.
    {
        const h = loadHelpers(js);
        const remembered = () => h.loadPasswordlessLogins().has(h.passwordlessLoginKey(server, user));
        h.rememberAuthenticationOutcome(response(403, 497), server, user, '');
        check('access-denied', 'ACCESS_DENIED with an empty password remembers it', remembered(), true);
        h.rememberAuthenticationOutcome(response(403, 497), server, user, 'secret');
        check('access-denied', 'ACCESS_DENIED with a real password forgets it', remembered(), false);
    }

    /// Contract 4: an error that did not come from the server proves nothing for an empty password.
    {
        const h = loadHelpers(js);
        const remembered = () => h.loadPasswordlessLogins().has(h.passwordlessLoginKey(server, user));
        h.rememberAuthenticationOutcome(response(502), server, user, '');
        check('proxy-error', 'a proxy error with an empty password is not remembered', remembered(), false);
        h.rememberAuthenticationOutcome(response(200), server, user, '');
        h.rememberAuthenticationOutcome(response(502), server, user, 'secret');
        check('proxy-error', 'a proxy error with a real password still forgets it', remembered(), false);
    }

    if (failures) {
        console.log(`${failures} check(s) failed`);
        process.exit(1);
    }
    console.log('All scenarios passed');
}

main().catch(e => {
    console.log(`FAIL harness error: ${e.stack || e}`);
    process.exit(1);
});
