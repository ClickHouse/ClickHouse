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
    const api = vm.runInNewContext(extract('const MAX_FLAME_NODES', 'async function getServerStatus')
        + extract('function makeEventStreamHandler(', '/// Parse one SSE event block')
        + '\n({freshFlameGraphState, accumulateProfileTraces, renderFlameGraph, makeEventStreamHandler, MAX_FLAME_NODES, MAX_FLAME_LABEL_CHARS, MAX_FLAME_DOM_FRAMES})',
        { document, _logHash: () => 0 });
    const sample = (symbols, trace_type = 'CPU', size = '0', trace = symbols.map((_, i) => String(i + 1)), host_name = 'host') =>
        ({ host_name, query_id: 'query', thread_id: '1', event_time_microseconds: '1788690000000000', symbols, trace, trace_type, size });
    const state = api.freshFlameGraphState();
    const handle = api.makeEventStreamHandler({ appendProfileTraces: batch => api.accumulateProfileTraces(state, batch) });
    handle('profile_traces', [JSON.stringify([sample(['leaf;with separator', 'root']), sample(['leaf;with separator', 'root'])])]);
    const cpu = state.types.get('CPU');
    assert.equal(cpu.root.value, 2);
    assert.equal(cpu.root.children.get('root').children.get('leaf;with separator').value, 2);
    handle('profile_traces', [JSON.stringify([sample(['other', 'root']), sample(['real'], 'Real'), sample(['alloc'], 'MemorySample', '128'), sample(['alloc'], 'MemorySample', '-128')])]);
    assert.equal(cpu.root.value, 3);
    assert.equal(state.types.get('Real').root.value, 1);
    assert.equal(state.types.get('MemorySample').root.value, 128);
    assert.equal(state.types.get('MemorySample').samples, 1);
    console.log('PASS event batches accumulate leaf-first CPU, Real, and positive allocation weights');

    const addresses = api.freshFlameGraphState();
    api.accumulateProfileTraces(addresses, [sample([''], 'CPU', '0', ['18446744073709551614']), sample([''], 'CPU', '0', ['18446744073709551615']), sample([''], 'CPU', '0', ['18446744073709551615'], 'other-host')]);
    assert.equal(addresses.types.get('CPU').root.children.size, 3);
    assert.ok(addresses.types.get('CPU').root.children.has('host: 18446744073709551615'));
    console.log('PASS UInt64 addresses stay distinct and unresolved addresses are host-scoped');

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

    const bounded = api.freshFlameGraphState();
    api.accumulateProfileTraces(bounded, Array.from({ length: api.MAX_FLAME_NODES + 5 }, (_, i) => sample(['frame-' + i])));
    assert.equal(bounded.nodes, api.MAX_FLAME_NODES);
    assert.equal(bounded.dropped, 5);
    api.accumulateProfileTraces(bounded, [sample(['frame-0'])]);
    assert.equal(bounded.types.get('CPU').root.children.get('frame-0').value, 2);
    const labels = api.freshFlameGraphState();
    api.accumulateProfileTraces(labels, [sample(['x'.repeat(api.MAX_FLAME_LABEL_CHARS + 1)]), sample(Array(257).fill('deep'))]);
    assert.equal(labels.nodes, 0);
    assert.equal(labels.dropped, 2);
    console.log('PASS node, symbol, and depth budgets reject whole stacks while existing stacks keep accumulating');

    const large = api.freshFlameGraphState();
    api.accumulateProfileTraces(large, Array.from({ length: 500 }, (_, i) => sample(['leaf', 'c', 'b', 'a', 'root-' + i])));
    api.renderFlameGraph(graph, large.types.get('CPU'), 'samples');
    assert.equal(graph.children.filter(frame => frame.className?.startsWith('flame-frame')).length, api.MAX_FLAME_DOM_FRAMES);
    assert.ok(graph.children.some(frame => frame.textContent.includes('Frame display limit')));
    assert.equal(api.freshFlameGraphState().types.size, 0);
    console.log('PASS DOM frame budget and independent fresh query state');
    console.log('All scenarios passed');
}

main().catch(error => { console.error(error); process.exitCode = 1; });
