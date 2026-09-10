#!/usr/bin/env node
'use strict';

/// Run the profiler helpers from the served page, or a local `programs/server/play.html`.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

class Element
{
    constructor()
    {
        this.children = [];
        this.style = {};
        this.listeners = {};
        this.classList = { add() {}, remove() {} };
    }
    appendChild(child)
    {
        if (child.fragment) this.children.push(...child.children);
        else this.children.push(child);
    }
    replaceChildren() { this.children = []; }
    addEventListener(type, fn) { this.listeners[type] = fn; }
    set innerHTML(value) { throw new Error('Untrusted symbols must not be parsed as HTML: ' + value); }
}

async function main()
{
    const source = process.argv[2];
    if (!source) throw new Error('Usage: node flamegraph_harness.js <page-file-or-url>');
    const html = /^https?:/.test(source) ? await (await fetch(source)).text() : fs.readFileSync(source, 'utf8');
    const js = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/g)].map(m => m[1]).sort((a, b) => b.length - a.length)[0];
    new vm.Script(js);
    const extract = (start, end) =>
    {
        const begin = js.indexOf(start);
        const finish = js.indexOf(end, begin);
        assert.ok(begin >= 0 && finish > begin, `Missing helper boundaries: ${start}`);
        return js.slice(begin, finish);
    };
    const document = {
        createElement: () => new Element(),
        createDocumentFragment: () => Object.assign(new Element(), { fragment: true }),
    };
    const requestApi = vm.runInNewContext(extract('const TT = {', '/// SQL keywords recognized')
        + extract('const OPENING_BRACKETS', '/// The closing type that matches')
        + extract('const TT_FALLBACK_OTHER', 'async function getQueryUnderCursor(')
        + extract('function profilerPeriodNs(', 'const MAX_FLAME_NODES')
        + extract('async function postImpl(', '    targetResultEl.queryText = query;')
        + '\nlet response;\n'
        + extract('        if (profile_traces && (', '        /// Detect image results')
        + '\nreturn {...response, profile_traces}; }\n'
        + '({detectFramingSetting, postImpl})', {
            tokenizeOrNull: async () => null,
            fetch: async (url, options) => ({url, options}),
            default_format: 'JSONStringsEachRowWithProgress',
            framed_default_format: 'JSONCompactStringsEachRowWithNamesAndTypes',
        });
    const request = (query, params = {}, enabled = true) => requestApi.postImpl(
        {profileTraces: enabled, profilerPeriodNs: '1000000'}, 1, query, {}, {}, params, '',
        {url: 'http://fixture/', user: '', password: ''}, 0);
    for (const value of ['0', "'0'", 'FALSE', "'false'", 'DEFAULT', "'\\x66alse'", '$value$false$value$'])
    {
        const result = await request(`SELECT 1 SETTINGS send_profile_traces = ${value}`);
        const params = new URL(result.url).searchParams;
        assert.equal(result.profile_traces, false, value);
        for (const name of ['send_profile_traces', 'query_profiler_cpu_time_period_ns', 'query_profiler_real_time_period_ns'])
            assert.equal(params.has(name), false, `${value}: ${name}`);
    }
    for (const query of [
        'SELECT 1 SETTINGS send_profile_traces = 1, send_profile_traces = 0',
        'SELECT 1 SETTINGS send_profile_traces = DEFAULT, send_profile_traces = 1',
        'SELECT 1 SETTINGS send_profile_traces = 1, send_profile_traces = DEFAULT',
        'SELECT 1 SETTINGS `send_profile_traces` = 0',
        'SELECT 1 SETTINGS optimize_move_to_prewhere, send_profile_traces = 0',
        'SELECT 1 SETTINGS send_profile_traces = {enabled:Bool}',
        'SET send_profile_traces = 0',
        "SET send_profile_traces = 'false'",
        'SET send_profile_traces = DEFAULT',
        'SET send_profile_traces = DEFAULT, send_profile_traces = 1',
        'SET send_profile_traces = 1, send_profile_traces = DEFAULT',
        'SET send_profile_traces = {enabled:Bool}',
    ])
        assert.equal((await request(query, {enabled: '0'})).profile_traces, false, query);
    for (const query of [
        "SELECT 'SETTINGS send_profile_traces = 0'",
        'SELECT 1 /* SETTINGS send_profile_traces = 0 */',
        'SELECT * FROM (SELECT 1 SETTINGS send_profile_traces = 0)',
        'SELECT settings x, send_profile_traces = 0 FROM t',
        'SELECT 1 SETTINGS send_profile_traces = 0, send_profile_traces = 1',
        'SELECT 1 SETTINGS send_profile_traces = 0, send_profile_traces',
        'SELECT 1 SETTINGS send_profile_traces = {enabled:Bool}',
        'SET send_profile_traces = 1',
        'SET send_profile_traces = 0, send_profile_traces = 1',
        'SET send_profile_traces = 0, send_profile_traces',
        "SELECT 'SET send_profile_traces = 0'",
    ])
    {
        const result = await request(query, {enabled: '1'});
        assert.equal(result.profile_traces, true, query);
        assert.equal(new URL(result.url).searchParams.get('query_profiler_cpu_time_period_ns'), '1000000', query);
    }
    const inlinePayload = await requestApi.detectFramingSetting("INSERT INTO FUNCTION null('line String') FORMAT LineAsString\nSETTINGS send_profile_traces = 0");
    assert.equal(!!inlinePayload.user_disables_profile_traces, false);
    assert.equal(inlinePayload.has_ambiguous_post_format_settings, true);
    assert.equal(new URL((await request('SELECT 1', {}, false)).url).searchParams.has('send_profile_traces'), false);
    const changingTab = {profileTraces: true, profilerPeriodNs: '1000000'};
    const pendingRequest = requestApi.postImpl(changingTab, 1, 'SELECT 1', {}, {}, {}, '', {url: 'http://fixture/'}, 0);
    changingTab.profileTraces = false;
    changingTab.profilerPeriodNs = '100000000';
    assert.equal(new URL((await pendingRequest).url).searchParams.get('query_profiler_cpu_time_period_ns'), '1000000');
    const sessionTab = {profileTraces: true, profilerPeriodNs: '1000000'};
    const sessionRequest = query => requestApi.postImpl(sessionTab, 1, query, {}, {}, {}, '', {url: 'http://fixture/'}, 0);
    assert.equal((await sessionRequest('SET send_profile_traces = 0')).profile_traces, false);
    assert.equal(sessionTab.profileTraces, true);
    const followingRequest = await sessionRequest('SELECT 1');
    assert.equal(followingRequest.profile_traces, true);
    assert.equal(new URL(followingRequest.url).searchParams.get('query_profiler_cpu_time_period_ns'), '1000000');
    await assert.rejects(sessionRequest("SET framing_output_format = 'EventStream'"), /whole session/);
    console.log('PASS request settings respect SQL opt-outs, DEFAULT resets, parameters, and lexical query scope');
    for (const clause of [
        "framing_output_format = 'EventStream'",
        "framing_output_format = 'Event\\x53tream'",
        'framing_output_format = $fmt$EventStream$fmt$',
        "framing_output_format = 'None', framing_output_format = 'EventStream'",
    ])
    {
        for (const enabled of [false, true])
        {
            const query = `SELECT 1 SETTINGS ${clause}`;
            const result = await request(query, {}, enabled);
            checkRequest(result, query, enabled, 'None');
        }
    }
    function checkRequest(result, query, profiling, framing, logs = true, format = 'JSONCompactStringsEachRowWithNamesAndTypes')
    {
        const params = new URL(result.url).searchParams;
        assert.equal(result.options.method, 'POST');
        assert.equal(result.options.body, query);
        assert.equal(result.profile_traces, profiling, query);
        assert.deepEqual(params.getAll('framing_output_format'), [framing], query);
        assert.deepEqual(params.getAll('default_format'), [format], query);
        assert.deepEqual(params.getAll('send_logs_level'), logs ? ['trace'] : [], query);
        assert.deepEqual(params.getAll('send_profile_traces'), profiling ? ['1'] : [], query);
        for (const name of ['query_profiler_cpu_time_period_ns', 'query_profiler_real_time_period_ns'])
            assert.deepEqual(params.getAll(name), profiling ? ['1000000'] : [], `${query}: ${name}`);
    }
    for (const enabled of [false, true])
    {
        checkRequest(await request('SELECT 1', {}, enabled), 'SELECT 1', enabled, 'EventStream');
        for (const value of ['0', 'DEFAULT'])
        {
            const query = `SELECT 1 SETTINGS framing_output_format = 'EventStream', send_profile_traces = ${value}`;
            checkRequest(await request(query, {}, enabled), query, false, 'None');
        }
    }
    const sessionQuery = "SELECT 1 SETTINGS framing_output_format = 'EventStream', send_logs_level = 'none'";
    const sessionResult = await requestApi.postImpl(
        sessionTab, 1, sessionQuery, {}, {}, {}, '', {url: 'http://fixture/?session_id=logs'}, 0);
    checkRequest(sessionResult, sessionQuery, true, 'None');
    assert.equal(new URL(sessionResult.url).searchParams.get('session_id'), 'logs');
    const packetQuery = "SELECT 1 SETTINGS framing_output_format = 'JSONEachPacketString'";
    checkRequest(await request(packetQuery, {}, false), packetQuery, false, 'None', false);
    const chartQuery = 'SELECT 1 FORMAT JSONCompactColumns';
    checkRequest(await request(chartQuery, {}, false), chartQuery, false, 'None', false, 'JSONStringsEachRowWithProgress');
    for (const query of [
        "SELECT 1 SETTINGS framing_output_format = 'None'",
        'SELECT 1 SETTINGS framing_output_format = DEFAULT',
        "SELECT 1 SETTINGS framing_output_format = 'JSONEachPacketString'",
        "SELECT 1 SETTINGS framing_output_format = 'EventStream', framing_output_format = 'None'",
        "SELECT 1 SETTINGS framing_output_format = DEFAULT, framing_output_format = 'EventStream'",
        "SELECT 1 FORMAT JSONCompactColumns SETTINGS framing_output_format = 'EventStream'",
    ])
        await assert.rejects(request(query), /framing/, query);
    console.log('PASS explicit EventStream preserves logs and request settings while incompatible framing and chart formats remain rejected');

    const api = vm.runInNewContext(extract('const MAX_FLAME_NODES', 'async function getServerStatus')
        + extract('function makeEventStreamHandler(', '/// Parse one SSE event block')
        + '\n({freshFlameGraphState, accumulateProfileTraces, selectFlameTree, updateFlameHostSelector, flameGraphStatus, renderFlameGraph, makeEventStreamHandler, MAX_FLAME_NODES, MAX_FLAME_LABEL_CHARS, MAX_FLAME_HOSTS, MAX_FLAME_DOM_FRAMES, MAX_FLAME_SERVER_DROPS})',
        { document, _logHash: () => 0 });
    const sample = (symbols, trace_type = 'CPU', size = '0', trace = symbols.map((_, i) => String(i + 1)), host_name = 'host') =>
        ({ host_name, query_id: 'query', thread_id: '1', event_time_microseconds: '1788690000000000', symbols, trace, trace_type, size });
    const state = api.freshFlameGraphState();
    const handle = api.makeEventStreamHandler({ appendProfileTraces: batch => api.accumulateProfileTraces(state, batch) });
    handle('profile_traces', [JSON.stringify([sample(['leaf;with separator', 'root']), sample(['leaf;with separator', 'root'])])]);
    const cpu = state.types.get('CPU');
    assert.equal(cpu.root.value, 2);
    assert.equal(cpu.root.children.get('sroot').children.get('sleaf;with separator').value, 2);
    handle('profile_traces', [JSON.stringify([sample(['other', 'root']), sample(['real'], 'Real'), sample(['alloc'], 'MemorySample', '128'), sample(['alloc'], 'MemorySample', '-128')])]);
    assert.equal(cpu.root.value, 3);
    assert.equal(state.types.get('Real').root.value, 1);
    assert.equal(state.types.get('MemorySample').root.value, 128);
    assert.equal(state.types.get('MemorySample').samples, 1);
    console.log('PASS event batches accumulate leaf-first CPU, Real, and positive allocation weights');

    const addresses = api.freshFlameGraphState();
    api.accumulateProfileTraces(addresses, [sample([''], 'CPU', '0', ['18446744073709551614']), sample([''], 'CPU', '0', ['18446744073709551615']), sample([''], 'CPU', '0', ['18446744073709551615'], 'other-host')]);
    assert.equal(addresses.types.get('CPU').root.children.size, 3);
    assert.ok([...addresses.types.get('CPU').root.children.values()].some(node => node.name === 'host: 18446744073709551615'));
    api.accumulateProfileTraces(addresses, [sample(['host: 18446744073709551615'])]);
    assert.equal(addresses.types.get('CPU').root.children.size, 4);
    console.log('PASS UInt64 addresses stay distinct, are host-scoped, and cannot collide with symbol labels');

    const hosts = api.freshFlameGraphState();
    api.accumulateProfileTraces(hosts, [sample(['shared', 'root'], 'CPU', '0', ['1', '2'], 'node-b'),
        sample(['shared', 'root'], 'CPU', '0', ['1', '2'], 'node-a'),
        sample(['other', 'root'], 'CPU', '0', ['3', '2'], 'node-a'),
        sample(['allocation'], 'MemorySample', '128', ['4'], 'node-a'),
        sample(['allocation'], 'MemorySample', '256', ['4'], 'node-b')]);
    const hostA = api.selectFlameTree(hosts, 'CPU', 'node-a');
    const hostB = api.selectFlameTree(hosts, 'CPU', 'node-b');
    assert.equal(api.selectFlameTree(hosts, 'CPU').root.value, 3);
    assert.equal(hostA.root.value, 2);
    assert.equal(hostB.root.value, 1);
    assert.equal(api.selectFlameTree(hosts, 'MemorySample').root.value, 384);
    assert.equal(api.selectFlameTree(hosts, 'MemorySample', 'node-b').root.value, 256);
    const selector = new Element();
    api.updateFlameHostSelector(selector, hosts);
    assert.deepEqual(selector.children.map(option => option.textContent), ['All nodes', 'node-a', 'node-b']);
    selector.value = ':node-a';
    const originalOptions = selector.children;
    api.updateFlameHostSelector(selector, hosts);
    assert.equal(selector.children, originalOptions);
    api.accumulateProfileTraces(hosts, [sample(['unknown'], 'Real', '0', ['5'], ''),
        sample(['hostile'], 'Real', '0', ['6'], '<img src=x onerror=alert(1)>')]);
    api.updateFlameHostSelector(selector, hosts);
    assert.equal(selector.value, ':node-a');
    assert.ok(selector.children.some(option => option.value === ':' && option.textContent === '(unknown node)'));
    assert.ok(selector.children.some(option => option.textContent === '<img src=x onerror=alert(1)>'));
    console.log('PASS all-node and per-node trees agree; selector preserves selection and treats host names as text');

    const losses = api.freshFlameGraphState();
    const lossHandler = api.makeEventStreamHandler({ appendProfileTraces: batch => api.accumulateProfileTraces(losses, batch) });
    lossHandler('profile_traces', [JSON.stringify([sample([], 'Dropped', '9007199254740993', [], 'reporter'),
        sample([], 'Dropped', '2', [], 'reporter'), sample([], 'Incomplete', '0', [], 'reporter')])]);
    assert.equal(losses.serverDropped, 9007199254740995n);
    assert.equal(losses.incomplete, true);
    assert.equal(losses.hosts.size, 0);
    assert.equal(losses.types.size, 0);
    api.accumulateProfileTraces(losses, [sample(['cpu'], 'CPU', '0', ['1'], 'node-a'), sample(Array(257).fill('deep'))]);
    const lossStatus = api.flameGraphStatus(losses, 'CPU', 'node-a', true);
    assert.ok(lossStatus.includes('9,007,199,254,740,995 samples lost in server queues (all nodes)'));
    assert.ok(lossStatus.includes('additional losses are unknown'));
    assert.ok(lossStatus.includes('1 samples omitted at the browser memory limit'));
    api.accumulateProfileTraces(losses, [sample([], 'Dropped', '9223372036854775807', [], 'another-reporter')]);
    assert.equal(losses.serverDropped, api.MAX_FLAME_SERVER_DROPS);
    assert.ok(api.flameGraphStatus(losses, 'Real', null, true).includes('At least 9,223,372,036,854,775,807'));
    assert.equal(losses.hosts.size, 1);
    console.log('PASS loss markers retain exact bounded global counts and completeness separately from browser omissions');

    const graph = new Element();
    api.renderFlameGraph(graph, cpu, 'samples');
    graph.children.find(frame => frame.textContent === 'leaf;with separator').listeners.click();
    const zoom = cpu.zoom;
    api.accumulateProfileTraces(state, [sample(['leaf;with separator', 'root'])]);
    api.renderFlameGraph(graph, cpu, 'samples');
    assert.equal(cpu.zoom, zoom);
    assert.equal(zoom.value, 3);
    assert.equal(graph.children.find(frame => frame.textContent === zoom.name).style.width, '100%');
    api.accumulateProfileTraces(state, [sample(['<img src=x onerror=alert(1)>'])]);
    cpu.zoom = cpu.root;
    api.renderFlameGraph(graph, cpu, 'samples');
    assert.ok(graph.children.some(frame => frame.textContent === '<img src=x onerror=alert(1)>'));
    console.log('PASS rendering preserves zoom across batches and treats symbols as text');

    api.renderFlameGraph(graph, hostA, 'samples');
    graph.children.find(frame => frame.textContent === 'shared').listeners.click();
    const hostZoom = hostA.zoom;
    api.renderFlameGraph(graph, hostB, 'samples');
    assert.equal(hostB.zoom, hostB.root);
    api.accumulateProfileTraces(hosts, [sample(['shared', 'root'], 'CPU', '0', ['1', '2'], 'node-a')]);
    api.renderFlameGraph(graph, hostA, 'samples');
    assert.equal(hostA.zoom, hostZoom);
    assert.equal(hostZoom.value, 2);
    assert.equal(graph.children.find(frame => frame.textContent === 'shared').style.width, '100%');
    console.log('PASS each node retains independent zoom across switching and new batches');

    const bounded = api.freshFlameGraphState();
    api.accumulateProfileTraces(bounded, Array.from({ length: api.MAX_FLAME_NODES + 5 }, (_, i) => sample(['frame-' + i])));
    assert.equal(bounded.nodes, api.MAX_FLAME_NODES);
    assert.equal(bounded.dropped, api.MAX_FLAME_NODES + 5 - bounded.types.get('CPU').samples);
    assert.equal(api.selectFlameTree(bounded, 'CPU', 'host').root.value, bounded.types.get('CPU').root.value);
    const beforeRejectedHost = bounded.types.get('CPU').root.value;
    api.accumulateProfileTraces(bounded, [sample(['frame-0'], 'CPU', '0', ['1'], 'late-host')]);
    assert.equal(bounded.types.get('CPU').root.value, beforeRejectedHost);
    assert.equal(bounded.hosts.has('late-host'), false);
    api.accumulateProfileTraces(bounded, [sample(['frame-0'])]);
    assert.equal(bounded.types.get('CPU').root.children.get('sframe-0').value, 2);
    const labels = api.freshFlameGraphState();
    api.accumulateProfileTraces(labels, [sample(['x'.repeat(api.MAX_FLAME_LABEL_CHARS + 1)]), sample(Array(257).fill('deep'))]);
    assert.equal(labels.nodes, 0);
    assert.equal(labels.hosts.size, 0);
    assert.equal(labels.types.size, 0);
    assert.equal(labels.dropped, 2);
    console.log('PASS shared node, symbol, and depth budgets reject both representations atomically');

    const manyHosts = api.freshFlameGraphState();
    api.accumulateProfileTraces(manyHosts, Array.from({ length: api.MAX_FLAME_HOSTS + 3 }, (_, i) =>
        sample(['shared'], 'CPU', '0', ['1'], 'node-' + i)));
    assert.equal(manyHosts.hosts.size, api.MAX_FLAME_HOSTS);
    assert.equal(manyHosts.types.get('CPU').samples, api.MAX_FLAME_HOSTS);
    assert.equal(manyHosts.dropped, 3);
    api.accumulateProfileTraces(manyHosts, [sample(['shared'], 'CPU', '0', ['1'], 'node-0')]);
    assert.equal(api.selectFlameTree(manyHosts, 'CPU', 'node-0').samples, 2);
    const hugeHost = api.freshFlameGraphState();
    api.accumulateProfileTraces(hugeHost, [sample(['shared'], 'CPU', '0', ['1'], 'h'.repeat(api.MAX_FLAME_LABEL_CHARS))]);
    assert.equal(hugeHost.hosts.size, 0);
    assert.equal(hugeHost.types.size, 0);
    assert.equal(hugeHost.dropped, 1);
    console.log('PASS host count and host text share bounded storage without dropping established hosts');

    const large = api.freshFlameGraphState();
    api.accumulateProfileTraces(large, Array.from({ length: 500 }, (_, i) => sample(['leaf', 'c', 'b', 'a', 'root-' + i])));
    api.renderFlameGraph(graph, large.types.get('CPU'), 'samples');
    assert.equal(graph.children.filter(frame => frame.className?.startsWith('flame-frame')).length, api.MAX_FLAME_DOM_FRAMES);
    assert.ok(graph.children.some(frame => frame.textContent.includes('Frame display limit')));
    assert.equal(api.freshFlameGraphState().types.size, 0);
    console.log('PASS DOM frame budget and independent fresh query state');

    const Result = vm.runInNewContext('(class {'
        + extract("    clear()\n    {\n        /// This result's rows", '    /// Select a cell and move keyboard focus to it.')
        + extract('    _showFlameGraph()\n', '    /// Queue a server log entry')
        + '})', { ...api, hideImagePreviewOwnedBy() {}, clearTimeout() {}, profilerPeriodNs() { return ''; } });
    const result = new Result();
    for (const field of ['_dataTable', '_graph', '_chart', '_dataUnparsed', '_error', '_dataDiv', '_pager', '_logsContent',
        '_flameGraph', '_flameStatus', '_flameHost', '_flameType', '_flamePeriod', '_metricsTable', '_metricsBody', '_resultGroup', '_logsDiv', '_metricsDiv',
        '_flameDiv', '_viewToggle', '_btnLogs', '_btnResult'])
        result[field] = new Element();
    result._clearElement = element => element.replaceChildren();
    result._clearImage = () => {};
    result._view = 'flame';
    result._flameDiv.style.display = 'block';
    result._flame_data = hosts;
    result._flameType.value = 'CPU';
    result._showFlameGraph();
    result._flameHost.value = ':node-b';
    result._showFlameGraph();
    assert.equal(result._flameStatus.textContent, '1 samples received.');
    result._flameType.value = 'MemorySample';
    result._showFlameGraph();
    assert.ok(result._flameGraph.children.some(frame => frame.title.startsWith('allocation\n256 B')));
    result.clear();
    assert.equal(result._view, 'result');
    assert.equal(result._resultGroup.style.display, '');
    assert.equal(result._flameDiv.style.display, 'none');
    assert.equal(result._flame_data.types.size, 0);
    assert.equal(result._flame_data.hosts.size, 0);
    assert.equal(result._flame_data.serverDropped, 0n);
    assert.equal(result._flame_data.incomplete, false);
    assert.equal(result._flameHost.value, '');
    assert.equal(result._flameHost.children.length, 1);
    console.log('PASS result controls select node/type and clearing resets samples, losses, selection, and zoom');
    console.log('All scenarios passed');
}

main().catch(error => { console.error(error); process.exitCode = 1; });
