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

/// A `localStorage` stand-in; passing the same one to two contexts simulates two visits of the page.
function makeStorage() {
    const items = new Map();
    return {
        getItem: key => items.has(key) ? items.get(key) : null,
        setItem: (key, value) => { items.set(key, String(value)); },
    };
}

/// A connection field: `classList` for the credential indication of `checkCredentials`, and `matches`
/// for the autofill detection (`isAutofilled`), which reports the field as autofilled while
/// `autofilled` is set.
function makeField() {
    const field = { value: '', autofilled: false, classList: { add() {}, remove() {} } };
    field.matches = selector => selector === ':autofill' && field.autofilled;
    return field;
}

function makeContext(storage) {
    const ctx = {
        localStorage: storage,
        window: { localStorage: storage },
        url_elem: makeField(),
        user_elem: makeField(),
        password_elem: makeField(),
        location: { href: 'http://localhost:8123/play', origin: 'http://localhost:8123' },
        stored: [],
        fetched: [],
        json: undefined,
        /// What the fake server reports as `currentUser` for a request without a `user` parameter
        /// (its `default_session_user`); `null` makes the status request fail.
        implicit_user: 'default',
        /// When set, a proxy in front of the fake server authenticates every request as this user with
        /// the `X-ClickHouse-User` / `X-ClickHouse-Key` headers, and, like the server, a request that
        /// also carries a `user` parameter is rejected.
        proxy_user: null,
        URL,
        console,
        encodeURIComponent,
        decodeURIComponent,
        JSON,
        Object,
        /// What the page computes without `CSS.supports`: no field is ever reported as autofilled, so
        /// `dropAutofilledStalePassword` (called by `storeCredentials`) keeps every password here.
        /// A scenario enables the detection by setting it to `:autofill` (see `makeField`).
        AUTOFILL_SELECTOR: null,
        PASSWORDLESS_LOGINS_KEY: 'passwordless_logins',
        /// The bounded lifetime of an implicit-user probe (`learnImplicitUser`).
        IMPLICIT_USER_PROBE_TIMEOUT_MS: 5000,
        AbortController,
        setTimeout,
        clearTimeout,
        MAX_PASSWORDLESS_LOGINS: 100,
        /// The page state `checkCredentials` reads and writes besides the connection fields.
        check_credentials_request_num: 0,
        checked_connection: null,
        document: { getElementById: () => ({ innerText: '' }) },
        formatUptime: () => '',
        updateServerInfoVisibility() {},
        openConnectionDropdown() {},
        /// The page state `restampPendingImplicitUser` writes again once this visit learns the
        /// implicit name: the current history entry, the tabs, and recorders for the two writers.
        history: { state: null },
        tabs: [],
        activeTab: null,
        refreshed: [],
        saves: 0,
    };
    ctx.getActiveTab = () => ctx.activeTab;
    ctx.refreshCurrentHistoryEntry = tab => { ctx.refreshed.push(tab); };
    ctx.scheduleSave = () => { ++ctx.saves; };
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
        if (ctx.proxy_user !== null && m) return { ok: false, json: async () => ({}) };
        const u = ctx.proxy_user !== null ? ctx.proxy_user : m ? decodeURIComponent(m[1]) : ctx.implicit_user;
        return { ok: true, json: async () => ({ v: 'test', t: 0, u }) };
    };
    vm.createContext(ctx);
    return ctx;
}

const FUNCTIONS = ['effectiveConnectionUser', 'implicitUserStamp', 'implicitUserVisitState', 'pendingImplicitUserStamp',
    'resolvedImplicitUser', 'durableImplicitUserStamp', 'durableResultSnapshot', 'runEntryImplicitUser', 'replayConnectionUser',
    'learnImplicitUser', 'implicitUserProbeAdmission', 'admitRunQuery', 'admitImplicitUserProbe', 'sameConnectionUser', 'sameAsLiveConnectionUser',
    'serverIdentityKey', 'serverAdmissionKey', 'learnedImplicitUser', 'rememberImplicitUser', 'restampPendingImplicitUser',
    'serverAddressWithoutSession', 'storeCredentials', 'getServerStatus', 'buildCompletionUrl',
    'isAutofilled', 'dropAutofilledStalePassword', 'passwordlessLoginKey', 'loadPasswordlessLogins',
    'rememberLoginPassword', 'rememberAuthenticationOutcome', 'connectionTuple', 'checkCredentials'];

function boot(js, { withPasswordCredential = true, storage = makeStorage() } = {}) {
    const ctx = makeContext(storage);
    if (!withPasswordCredential) delete ctx.PasswordCredential;
    vm.runInContext(FUNCTIONS.map(name => extractFunction(js, name)).join('\n\n'), ctx);
    return ctx;
}

/// The wire URL of the requests the page issues for the current (url, user, password) fields.
async function requestUrls(ctx) {
    ctx.fetched.length = 0;
    await vm.runInContext('getServerStatus(url_elem.value, user_elem.value, password_elem.value)', ctx);
    const status_url = ctx.fetched.length ? ctx.fetched[0].url : null;
    const completion_url = vm.runInContext('buildCompletionUrl(url_elem.value, user_elem.value, password_elem.value)', ctx);
    return { status_url, completion_url };
}

function connectionIdentity(ctx) {
    return vm.runInContext('effectiveConnectionUser(url_elem.value, user_elem.value)', ctx);
}

/// The connection stamp the page records for the live fields (a result snapshot, a history entry,
/// a selection), as plain data, so it can outlive the context like a persisted entry does.
function stampConnection(ctx) {
    return JSON.parse(vm.runInContext(
        'JSON.stringify({server: url_elem.value, user: user_elem.value, implicitUser: implicitUserStamp(url_elem.value, user_elem.value)})', ctx));
}

/// The account the stamp of the live fields denotes (`resolvedImplicitUser`): empty while nothing is
/// learned, whether the stamp is empty or a marker of this visit.
function stampedAccount(ctx) {
    return vm.runInContext('resolvedImplicitUser(url_elem.value, implicitUserStamp(url_elem.value, user_elem.value))', ctx);
}

/// Do the identity gates (selected database, run divergence, history restore) see the stored
/// connection `entry` as the live one?
function sameAsLive(ctx, entry) {
    ctx.entry = entry;
    return vm.runInContext('sameAsLiveConnectionUser(entry.server, entry.user, entry.implicitUser)', ctx);
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
    /// Recorded before the name is learned, like the snapshot of a run launched on a fresh visit.
    const unstamped = stampConnection(ctx);
    if (!unstamped.implicitUser.startsWith('\u0000pending:'))
        throw new Error(`an implicit connection is not stamped with a visit marker before the name is learned: ${JSON.stringify(unstamped)}`);
    await store(ctx);
    assertEqual(ctx.fetched.length, 1, 'the implicit user is asked from the server');
    assertEqual(ctx.fetched[0].url, before.status_url, 'the probe is the empty-field request');
    if (!ctx.fetched[0].options.body.includes('currentUser()')) throw new Error(`probe does not ask currentUser: ${ctx.fetched[0].options.body}`);
    assertEqual(ctx.stored.length, 1, 'one credential stored');
    assertEqual(ctx.stored[0].id, implicit_user, 'remembered id is the server-reported implicit user');
    assertEqual(ctx.stored[0].password, 'secret', 'remembered password');
    assertEqual(ctx.stored[0].name, server, 'remembered name is the server URL');
    const stamped = stampConnection(ctx);
    assertEqual(stamped.implicitUser, implicit_user, 'an implicit connection is stamped with the learned user');

    ctx.user_elem.value = ctx.stored[0].id;
    const after = await requestUrls(ctx);
    const param = '&user=' + encodeURIComponent(implicit_user) + '&';
    if (!after.status_url.includes(param)) throw new Error(`refilled login does not select ${implicit_user}: ${after.status_url}`);
    if (!after.completion_url.includes(param)) throw new Error(`refilled login does not select ${implicit_user}: ${after.completion_url}`);
    const status = await vm.runInContext('getServerStatus(url_elem.value, user_elem.value, password_elem.value)', ctx);
    assertEqual(status.u, implicit_user, 'refilled login authenticates as the same user');

    /// The refilled login is the same connection as the implicit one it was saved from, so the
    /// identity gates (selected database, run divergence, history restore) see no change. That
    /// includes an entry recorded earlier in this visit, before the name was learned: its marker
    /// resolves to the name learned since (a run in flight while `storeCredentials` learns it).
    assertEqual(connectionIdentity(ctx), implicit_user, 'identity of the refilled login');
    assertEqual(sameAsLive(ctx, stamped), true, 'refilled login is the same connection as the stamped implicit one');
    assertEqual(sameAsLive(ctx, unstamped), true, 'refilled login is the same connection as an implicit one recorded earlier in this visit');
    ctx.user_elem.value = '';
    assertEqual(sameAsLive(ctx, stamped), true, 'implicit login is the same connection as the stamped implicit one');
    assertEqual(sameAsLive(ctx, unstamped), true, 'implicit login is the same connection as an implicit one recorded earlier in this visit');
    /// An entry with no name at all (written before the stamp existed) cannot be proven to be that
    /// account, so it fails closed.
    assertEqual(sameAsLive(ctx, { server, user: '', implicitUser: '' }), false,
        'implicit login with a learned name is not proven to be an unstamped implicit connection');
    return { stamped, unstamped };
}

scenario('implicit-default-round-trip', async js => {
    const ctx = boot(js);
    await implicitRoundTrip(ctx, 'http://host:8123/', 'default');
});

/// The password manager refills the login on the NEXT visit, where it is compared with history
/// entries stamped with the implicit user on the previous one: the stamp carries the name across the
/// reload, and a name learned for one server must not leak to a different one.
scenario('refilled-login-same-identity-after-reload', async js => {
    const storage = makeStorage();
    const first = boot(js, { storage });
    const { stamped: entry, unstamped } = await implicitRoundTrip(first, 'http://host:8123/', 'alice');
    const second = boot(js, { storage });
    second.implicit_user = 'alice';
    second.password_elem.value = 'secret';
    second.url_elem.value = 'http://host:8123';
    second.user_elem.value = 'alice';
    assertEqual(sameAsLive(second, entry), true, 'refilled login after reload (cosmetic URL difference)');
    /// A marker of the previous visit is not resolved by this one: it fails closed.
    assertEqual(sameAsLive(second, unstamped), false, 'refilled login after reload vs a marker of the previous visit');
    second.user_elem.value = 'bob';
    assertEqual(sameAsLive(second, entry), false, 'another explicit user after reload');
    second.user_elem.value = '';
    /// The name learned by the previous visit is not trusted for the live connection: the server may
    /// authenticate the empty field as someone else by now. Until this visit asks, the live implicit
    /// connection is only its own marker.
    if (!stampConnection(second).implicitUser.startsWith('\u0000pending:'))
        throw new Error(`the live implicit connection after reload is stamped before this visit asked: ${JSON.stringify(stampConnection(second))}`);
    assertEqual(sameAsLive(second, entry), false, 'implicit login after reload before the name is asked again');
    await store(second);
    assertEqual(stampConnection(second).implicitUser, 'alice', 'the name is learned again by this visit');
    assertEqual(sameAsLive(second, entry), true, 'implicit login after reload once the same name is learned');
    assertEqual(sameAsLive(second, unstamped), false, 'implicit login after reload vs a marker of the previous visit');
    second.url_elem.value = 'http://other:8123/';
    assertEqual(stampedAccount(second), '', 'nothing learned for another server');
    second.url_elem.value = 'http://host:8123/?cluster=b';
    assertEqual(stampedAccount(second), '', 'nothing learned for another query string');
});

/// A stored entry keeps denoting the account it was recorded with: when the server later reports a
/// different implicit user (its `default_session_user` changed), an old implicit entry must not be
/// reinterpreted as belonging to the newly learned name.
scenario('stored-implicit-entry-keeps-its-account', async js => {
    const storage = makeStorage();
    const first = boot(js, { storage });
    const { stamped: entry } = await implicitRoundTrip(first, 'http://host:8123/', 'alice');
    const second = boot(js, { storage });
    second.implicit_user = 'bob';
    second.url_elem.value = 'http://host:8123/';
    second.user_elem.value = '';
    second.password_elem.value = 'secret';
    /// Before this visit asks the server, nothing proves the empty field is still alice: the entry is
    /// not the live connection (the selected database is not kept for it), and a `run=1` link of a run
    /// launched now does not carry alice for a query that runs as bob.
    assertEqual(sameAsLive(second, entry), false, 'an entry recorded for alice before this visit asks the server');
    const early_run = stampConnection(second);
    second.entry = early_run;
    assertEqual(vm.runInContext('runEntryImplicitUser(entry.server, entry.user, entry.implicitUser)', second), '',
        'a run launched before this visit asks the server carries no account');
    await store(second);
    assertEqual(vm.runInContext('runEntryImplicitUser(entry.server, entry.user, entry.implicitUser)', second), 'bob',
        'and carries the account learned for it once this visit has asked');
    assertEqual(second.stored[0].id, 'bob', 'remembered under the newly reported implicit user');
    assertEqual(stampConnection(second).implicitUser, 'bob', 'new entries are stamped with the new implicit user');
    second.user_elem.value = 'bob';
    assertEqual(sameAsLive(second, entry), false, 'an entry recorded for alice is not the refilled bob');
    second.user_elem.value = 'alice';
    assertEqual(sameAsLive(second, entry), true, 'an entry recorded for alice is still the explicit alice');
    /// Implicit to implicit: the live empty field now authenticates as bob, so neither an entry
    /// stamped alice nor an entry recorded before any name was learned is the live connection.
    second.user_elem.value = '';
    assertEqual(sameAsLive(second, entry), false, 'an entry recorded for alice is not the implicit bob');
    assertEqual(sameAsLive(second, { server: 'http://host:8123/', user: '', implicitUser: '' }), false,
        'an unstamped implicit entry is not the implicit bob');
    /// Before a visit learns the name, the live implicit connection is only its own marker: an entry
    /// stamped with no name, or with a marker of an earlier visit (possibly for another account), is
    /// not proven to be it.
    const third = boot(js);
    third.url_elem.value = 'http://host:8123/';
    third.user_elem.value = '';
    const current = stampConnection(third);
    assertEqual(sameAsLive(third, current), true, 'an entry recorded earlier in this visit before the name is learned');
    assertEqual(sameAsLive(third, { server: 'http://host:8123/', user: '', implicitUser: '' }), false,
        'an unstamped implicit entry before the name is learned');
    assertEqual(sameAsLive(third, { server: 'http://host:8123/', user: '', implicitUser: '\u0000pending:earlier' }), false,
        'a marker of an earlier visit before the name is learned');
});

/// A run launched on a fresh visit with an empty field is snapshotted before the implicit user is
/// known; `storeCredentials` (or `checkCredentials`) learns it while the run is in flight. The
/// completed snapshot must still be the live connection, or the run would count as diverged and lose
/// `run=1`. The marker only takes the first name learned for that server, not later ones or other
/// servers' names.
scenario('run-snapshot-before-implicit-user-learned', async js => {
    const ctx = boot(js);
    ctx.implicit_user = 'alice';
    ctx.url_elem.value = 'http://host:8123/';
    ctx.user_elem.value = '';
    ctx.password_elem.value = 'secret';
    const run_snapshot = stampConnection(ctx);
    ctx.url_elem.value = 'http://other:8123/';
    const other_snapshot = stampConnection(ctx);
    assertEqual(sameAsLive(ctx, other_snapshot), true, 'same unknown implicit user on the other server');
    ctx.url_elem.value = 'http://host:8123/';
    assertEqual(sameAsLive(ctx, run_snapshot), true, 'the live connection before the name is learned');
    await store(ctx);
    assertEqual(stampConnection(ctx).implicitUser, 'alice', 'learned');
    assertEqual(sameAsLive(ctx, run_snapshot), true, 'the snapshot of the in-flight run is still the live connection');
    ctx.user_elem.value = 'alice';
    assertEqual(sameAsLive(ctx, run_snapshot), true, 'and the refilled login');
    ctx.user_elem.value = 'bob';
    assertEqual(sameAsLive(ctx, run_snapshot), false, 'but not another explicit user');
    ctx.user_elem.value = '';
    ctx.implicit_user = 'bob';
    await store(ctx);
    assertEqual(stampConnection(ctx).implicitUser, 'bob', 'relearned');
    assertEqual(sameAsLive(ctx, run_snapshot), false, 'the marker keeps the first learned account');
    ctx.url_elem.value = 'http://other:8123/';
    assertEqual(sameAsLive(ctx, other_snapshot), true, 'nothing learned for the other server yet');
    ctx.user_elem.value = 'alice';
    assertEqual(sameAsLive(ctx, other_snapshot), false, 'a name learned for one server does not resolve the marker for another');
});

/// An auto-executed `run=1` link must replay under the account that produced it, not under whatever
/// the server's `default_session_user` is when the link is opened: once the implicit user is known,
/// the link names it explicitly, including for a run snapshotted before the name was learned.
scenario('run-link-carries-implicit-account', async js => {
    const ctx = boot(js);
    ctx.implicit_user = 'alice';
    ctx.url_elem.value = 'http://host:8123/';
    ctx.user_elem.value = '';
    ctx.password_elem.value = 'secret';
    const run_snapshot = stampConnection(ctx);
    ctx.entry = run_snapshot;
    const runImplicitUser = () => vm.runInContext('runEntryImplicitUser(entry.server, entry.user, entry.implicitUser)', ctx);
    assertEqual(runImplicitUser(), '', 'nothing to carry before the implicit user is known');
    await store(ctx);
    assertEqual(runImplicitUser(), 'alice', 'the learned account of the in-flight run');
    ctx.entry = { ...run_snapshot, implicitUser: 'carol' };
    assertEqual(runImplicitUser(), 'carol', 'the account the stored entry was stamped with');
    ctx.entry = { server: 'http://host:8123/', user: 'bob', implicitUser: '' };
    assertEqual(runImplicitUser(), '', 'an explicit user needs no implicit account');
    ctx.entry = { server: 'http://dave@host:8123/', user: '', implicitUser: '' };
    assertEqual(runImplicitUser(), '', 'a userinfo connection needs no implicit account');
});

/// An empty field can be authenticated by a proxy injecting the `X-ClickHouse-User` / `X-ClickHouse-Key`
/// headers, and the server rejects a `user` parameter combined with them. Reproducing an implicit run
/// (Download) must keep the empty field while the server still authenticates it as the recorded
/// account, including on a later visit that has not learned the name yet, and only name the account
/// when the server now authenticates an empty field as somebody else.
scenario('proxy-authenticated-implicit-replay', async js => {
    const server = 'http://host:8123/';
    const first = boot(js);
    first.proxy_user = 'alice';
    first.url_elem.value = server;
    first.user_elem.value = '';
    first.password_elem.value = '';
    vm.runInContext('checkCredentials()', first);
    for (let i = 0; i < 5; ++i) await new Promise(resolve => setImmediate(resolve));
    const recorded = stampConnection(first);
    assertEqual(recorded.implicitUser, 'alice', 'the proxy-authenticated implicit run is stamped with its account');

    const replay = async (ctx, entry) => {
        ctx.entry = entry;
        ctx.fetched.length = 0;
        const user = await vm.runInContext('replayConnectionUser(entry.server, entry.user, entry.implicitUser, password_elem.value)', ctx);
        ctx.replay_user = user;
        const status = await vm.runInContext('getServerStatus(entry.server, replay_user, password_elem.value)', ctx);
        return { user, status };
    };
    let result = await replay(first, recorded);
    assertEqual(result.user, '', 'the replay keeps the empty field');
    assertEqual(result.status && result.status.u, 'alice', 'and is authenticated as the recorded account');

    /// A later visit (a restored result) asks the server first instead of naming the account.
    const later = boot(js);
    later.proxy_user = 'alice';
    later.url_elem.value = server;
    result = await replay(later, recorded);
    assertEqual(result.user, '', 'a later visit keeps the empty field too');
    assertEqual(result.status && result.status.u, 'alice', 'and is authenticated as the recorded account');

    /// The empty field is now authenticated as another account: the recorded one is named, so the
    /// request fails visibly instead of exporting the data of bob.
    const changed = boot(js);
    changed.proxy_user = 'bob';
    changed.url_elem.value = server;
    result = await replay(changed, recorded);
    assertEqual(result.user, 'alice', 'the recorded account is named when the implicit account changed');
    assertEqual(result.status, false, 'and the request is not served as bob');
});

/// A fast run can finish, and be recorded with its marker, before this visit learns the implicit name.
/// Learning the name then writes the current history entry and the persisted snapshots again, so the
/// `run=1` URL carries the account and the snapshot is stored with the name (`durableResultSnapshot`).
/// Nothing is written when no state carries a marker of this visit, or when the name was already known.
scenario('learned-implicit-user-restamps-recorded-run', async js => {
    const ctx = boot(js);
    ctx.implicit_user = 'alice';
    ctx.url_elem.value = 'http://host:8123/';
    ctx.user_elem.value = '';
    ctx.password_elem.value = 'secret';
    const run = stampConnection(ctx);
    const result = { url: run.server, user: '', implicitUser: run.implicitUser, query: 'SELECT 1' };
    const tab = { id: 't1', cells: [{ type: 'query', result }] };
    ctx.tabs = [tab, { id: 't2', cells: [{ type: 'text' }] }];
    ctx.activeTab = tab;
    ctx.history.state = { tabId: 't1', connectionServer: run.server, connectionUser: '',
        connectionImplicitUser: run.implicitUser, result };
    await store(ctx);
    assertEqual(ctx.refreshed.length, 1, 'the current entry recorded with the marker is refreshed');
    assertEqual(ctx.refreshed[0] === tab, true, 'the active tab is refreshed');
    assertEqual(ctx.saves, 1, 'the snapshots recorded with the marker are saved again');
    ctx.entry = run;
    assertEqual(vm.runInContext('runEntryImplicitUser(entry.server, entry.user, entry.implicitUser)', ctx), 'alice',
        'the refreshed `run=1` URL carries the account');
    await store(ctx);
    assertEqual(ctx.refreshed.length + ctx.saves, 2, 'nothing is written again once the name is known');

    const other = boot(js);
    other.implicit_user = 'alice';
    other.url_elem.value = 'http://host:8123/';
    other.user_elem.value = '';
    other.password_elem.value = 'secret';
    const stale = { url: 'http://host:8123/', user: '', implicitUser: '\u0000pending:earlier-visit' };
    other.activeTab = { id: 't1', cells: [{ type: 'query', result: stale }] };
    other.tabs = [other.activeTab];
    other.history.state = { tabId: 't1', connectionImplicitUser: stale.implicitUser, result: stale };
    await store(other);
    assertEqual(other.refreshed.length + other.saves, 0, 'state carrying no marker of this visit is not written');
});

/// A result snapshot taken before the implicit user is known carries a marker of this visit; it is
/// persisted (`IndexedDB`, `history.state`) as the name learned since, so after a reload it still
/// matches the same account (and a download or `run=1` link still names it), while a marker that has
/// not resolved is kept and fails closed.
scenario('persisted-result-stamp-is-durable', async js => {
    const ctx = boot(js);
    ctx.implicit_user = 'alice';
    ctx.url_elem.value = 'http://host:8123/';
    ctx.user_elem.value = '';
    ctx.password_elem.value = 'secret';
    ctx.result = { ...stampConnection(ctx), url: 'http://host:8123/', query: 'SELECT 1' };
    const persisted = () => JSON.parse(vm.runInContext('JSON.stringify(durableResultSnapshot(result))', ctx));
    assertEqual(persisted().implicitUser.startsWith('\u0000pending:'), true, 'an unresolved marker is kept');
    await store(ctx);
    const snapshot = persisted();
    assertEqual(snapshot.implicitUser, 'alice', 'the marker is persisted as the learned name');
    assertEqual(snapshot.query, 'SELECT 1', 'the rest of the snapshot is kept');

    const reloaded = boot(js);
    reloaded.implicit_user = 'alice';
    reloaded.url_elem.value = 'http://host:8123/';
    reloaded.user_elem.value = '';
    reloaded.password_elem.value = 'secret';
    await store(reloaded);
    assertEqual(sameAsLive(reloaded, { server: snapshot.url, user: '', implicitUser: snapshot.implicitUser }), true,
        'the persisted snapshot matches the same implicit account after a reload');
    reloaded.entry = { server: snapshot.url, user: '', implicitUser: snapshot.implicitUser };
    assertEqual(vm.runInContext('runEntryImplicitUser(entry.server, entry.user, entry.implicitUser)', reloaded), 'alice',
        'a `run=1` link of the persisted snapshot carries the producing account');
});

/// The probe is an ordinary query of the same user, so it must not compete with the query of the Run
/// for the user's query slots (`max_concurrent_queries_for_user = 1` rejects whichever arrives second
/// with `TOO_MANY_SIMULTANEOUS_QUERIES`): it is sent only once the Run has settled, failed or not, and
/// with the connection taken at launch.
scenario('probe-after-run-completes', async js => {
    for (const outcome of ['resolve', 'reject']) {
        const ctx = boot(js);
        ctx.implicit_user = 'alice';
        ctx.url_elem.value = 'http://host:8123/';
        ctx.user_elem.value = '';
        ctx.password_elem.value = 'secret';
        let finish;
        ctx.run = new Promise((resolve, reject) => { finish = outcome === 'resolve' ? resolve : reject; });
        const stored = vm.runInContext('storeCredentials(run)', ctx);
        ctx.url_elem.value = 'http://other:8123/';
        ctx.password_elem.value = 'other';
        for (let i = 0; i < 5; ++i) await new Promise(resolve => setImmediate(resolve));
        assertEqual(ctx.fetched.length, 0, `no probe while the Run is in flight (${outcome})`);
        assertEqual(ctx.stored.length, 0, `nothing stored while the Run is in flight (${outcome})`);
        finish(new Error('the Run failed'));
        await stored;
        assertEqual(ctx.fetched.length, 1, `one probe once the Run has settled (${outcome})`);
        if (!ctx.fetched[0].url.startsWith('http://host:8123/?') || !ctx.fetched[0].url.includes('&password=secret'))
            throw new Error(`the probe does not use the connection taken at launch: ${ctx.fetched[0].url}`);
        assertEqual(ctx.stored.length, 1, `stored once the Run has settled (${outcome})`);
        assertEqual(ctx.stored[0].id, 'alice', `remembered under the implicit user (${outcome})`);
        assertEqual(ctx.stored[0].name, 'http://host:8123/', `remembered for the server taken at launch (${outcome})`);
    }
});

/// The login of an empty field is saved under the name of the account (`storeCredentials`), so the
/// next visit refills that name. A later passwordless authentication of the empty field must be
/// recorded for that name as well, or the stale autofilled password of the refilled login is kept,
/// sent, and offered to the browser again. A password login of the empty field forgets it under both
/// spellings, and another user is not affected.
scenario('refill-after-passwordless-login', async js => {
    const settle = async () => { for (let i = 0; i < 5; ++i) await new Promise(resolve => setImmediate(resolve)); };
    const visit = (storage, user, password) => {
        const ctx = boot(js, { storage });
        ctx.AUTOFILL_SELECTOR = ':autofill';
        ctx.implicit_user = 'alice';
        ctx.url_elem.value = 'http://host:8123/';
        ctx.user_elem.value = user;
        ctx.password_elem.value = password;
        return ctx;
    };
    const storage = makeStorage();
    const first = visit(storage, '', 'secret');
    await store(first);
    assertEqual(first.stored[0].id, 'alice', 'the empty-field login is saved under the implicit user');

    /// The account's password was removed: the empty field authenticates with an empty password.
    const second = visit(storage, '', '');
    vm.runInContext('checkCredentials()', second);
    await settle();

    /// The password manager refills the saved login.
    const third = visit(storage, 'alice', 'secret');
    third.password_elem.autofilled = true;
    await store(third);
    assertEqual(third.password_elem.value, '', 'the stale autofilled password of the refilled login is dropped');
    assertEqual(third.stored.length, 0, 'and not offered to the browser again');

    /// Another user is not affected by the passwordless login of alice.
    const other = visit(storage, 'bob', 'secret');
    other.password_elem.autofilled = true;
    await store(other);
    assertEqual(other.password_elem.value, 'secret', 'the autofilled password of another user is kept');

    /// The account got a password back: a password login of the empty field forgets the entry for the
    /// refilled name too.
    const fourth = visit(storage, '', 'new');
    vm.runInContext('checkCredentials()', fourth);
    await settle();
    const fifth = visit(storage, 'alice', 'new');
    fifth.password_elem.autofilled = true;
    await store(fifth);
    assertEqual(fifth.password_elem.value, 'new', 'the autofilled password is kept after a password login');
    assertEqual(fifth.stored[0].id, 'alice', 'and offered to the browser');
});

/// The same, when the passwordless login is proven only by a query (`rememberAuthenticationOutcome`):
/// on routed deployments the ping of `checkURL` fails and `checkCredentials` never runs, so this visit
/// has not learned the implicit name, and the page asks the server for it.
scenario('refill-after-passwordless-query', async js => {
    const settle = async () => { for (let i = 0; i < 5; ++i) await new Promise(resolve => setImmediate(resolve)); };
    const visit = (storage, user, password) => {
        const ctx = boot(js, { storage });
        ctx.AUTOFILL_SELECTOR = ':autofill';
        ctx.implicit_user = 'alice';
        ctx.url_elem.value = 'http://host:8123/?session_id=s1';
        ctx.user_elem.value = user;
        ctx.password_elem.value = password;
        return ctx;
    };
    const query = (ctx) => {
        ctx.response = { ok: true, status: 200, headers: { get: () => null } };
        vm.runInContext('rememberAuthenticationOutcome(response, url_elem.value, user_elem.value, password_elem.value)', ctx);
    };
    const storage = makeStorage();
    const first = visit(storage, '', 'secret');
    await store(first);
    assertEqual(first.stored[0].id, 'alice', 'the empty-field login is saved under the implicit user');

    const second = visit(storage, '', '');
    query(second);
    query(second);
    await settle();
    assertEqual(second.fetched.length, 1, 'the implicit user is asked from the server once');
    if (second.fetched[0].url.includes('session_id')) throw new Error(`the probe is sent in the session: ${second.fetched[0].url}`);

    const third = visit(storage, 'alice', 'secret');
    third.password_elem.autofilled = true;
    await store(third);
    assertEqual(third.password_elem.value, '', 'the stale autofilled password of the refilled login is dropped');
    assertEqual(third.stored.length, 0, 'and not offered to the browser again');

    const other = visit(storage, 'bob', 'secret');
    other.password_elem.autofilled = true;
    await store(other);
    assertEqual(other.password_elem.value, 'secret', 'the autofilled password of another user is kept');

    const fourth = visit(storage, '', 'new');
    query(fourth);
    await settle();
    const fifth = visit(storage, 'alice', 'new');
    fifth.password_elem.autofilled = true;
    await store(fifth);
    assertEqual(fifth.password_elem.value, 'new', 'the autofilled password is kept after a password query');
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
