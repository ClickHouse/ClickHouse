#!/usr/bin/env bash
# Tags: distributed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The traceView table function renders the spans of one trace as a call tree with a
# timeline: one row per span in depth-first tree order, the tree on the left, and a
# fixed-width bar whose position is the span's start offset within the trace and whose
# length is proportional to its duration.

function poll_trace_view
{
    # Spans are flushed to the log by background threads: poll until traceView sees the
    # expected number of per-shard fragment spans. $1 - trace_id, $2 - expected count.
    local _trace_id="$1"
    local _expected="$2"
    local _count=0
    for _retry in {1..20}; do
        ${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
        _count=$(${CLICKHOUSE_CLIENT} -q "
            select countIf(span like '%RemoteQueryExecutor::execute%')
            from traceView('$_trace_id')" 2>/dev/null)
        [[ "${_count:-0}" -ge "$_expected" ]] && return 0
        sleep 1
    done
    echo "traceView did not see $_expected fragment spans in time, last count: $_count" >&2
    return 1
}

${CLICKHOUSE_CLIENT} -q "drop table if exists dist_over_two_shards_tv"
${CLICKHOUSE_CLIENT} -q "
    create table dist_over_two_shards_tv (dummy UInt8)
    engine = Distributed(test_cluster_two_shards, system, one)
"

trace_id_hex=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
trace_id=$(${CLICKHOUSE_CLIENT} -q "select toString(UUIDNumToString(toFixedString(unhex('$trace_id_hex'), 16)))")
query_id="$CLICKHOUSE_TEST_UNIQUE_NAME-tv"

# prefer_localhost_replica=0: both shards must be read through RemoteQueryExecutor,
# otherwise the first shard is read by a local plan and gets no fragment span.
${CLICKHOUSE_CLIENT} \
    --opentelemetry-traceparent "00-$trace_id_hex-0000000000000073-01" \
    --prefer_localhost_replica=0 \
    --query_id "$query_id" \
    --query "select * from dist_over_two_shards_tv format Null"

poll_trace_view "$trace_id" 2 || exit 1

echo "=== tree structure ==="
# Exactly one fragment span per shard, shard numbers rendered into the span text, and the
# tree connectors present (the fragment spans are children, so each carries a connector).
${CLICKHOUSE_CLIENT} -q "
    select
        if(countIf(span like '%RemoteQueryExecutor::execute  shard 1%') = 1
               and countIf(span like '%RemoteQueryExecutor::execute  shard 2%') = 1,
           'one fragment span per shard: OK', 'one fragment span per shard: FAIL'),
        if(countIf(span like '%├─%' or span like '%└─%') >= 2,
           'tree connectors: OK', 'tree connectors: FAIL')
    from traceView('$trace_id')
    format TSV
"

echo "=== timeline geometry ==="
# Every timeline is exactly timeline_width characters, contains at least one bar cell, and
# fits the trace: the root 'query' span starts at offset 0, spans start within the trace.
${CLICKHOUSE_CLIENT} -q "
    select
        if(countIf(lengthUTF8(timeline) != 40) = 0, 'default width 40: OK', 'default width 40: FAIL'),
        if(countIf(position(timeline, '█') = 0) = 0, 'bars non-empty: OK', 'bars non-empty: FAIL'),
        if(min(start_offset_us) = 0,
           'trace starts at offset 0: OK', 'trace starts at offset 0: FAIL'),
        if(countIf(self_pct < 0 or self_pct > 100) = 0, 'self_pct in [0, 100]: OK', 'self_pct in [0, 100]: FAIL'),
        if(countIf(duration_us > 3600 * 1000000) = 0,
           'durations sane: OK', 'durations sane: FAIL (a span start or finish time uses the wrong clock)')
    from traceView('$trace_id')
    format TSV
"

echo "=== custom width ==="
${CLICKHOUSE_CLIENT} -q "
    select if(countIf(lengthUTF8(timeline) != 20) = 0, 'width 20: OK', 'width 20: FAIL')
    from traceView('$trace_id', 20)
    format TSV
"

echo "=== lookup by query_id ==="
# The named query_id argument resolves to the same trace: same fragment spans, and the
# named timeline_width still applies.
${CLICKHOUSE_CLIENT} -q "
    select
        if(countIf(span like '%RemoteQueryExecutor::execute%') = 2,
           'query_id finds the trace: OK', 'query_id finds the trace: FAIL'),
        if(countIf(lengthUTF8(timeline) != 25) = 0, 'named width: OK', 'named width: FAIL')
    from traceView(query_id = '$query_id', timeline_width = 25)
    format TSV
"

echo "=== errors ==="
# Unknown trace: a clear error with the flush hint instead of an empty result.
${CLICKHOUSE_CLIENT} -q "select * from traceView('00000000-0000-0000-0000-000000000001')" 2>&1 \
    | grep -o -m1 'No spans found for trace_id' || echo 'missing-trace error: FAIL'
# Malformed trace_id.
${CLICKHOUSE_CLIENT} -q "select * from traceView('not-a-uuid')" 2>&1 \
    | grep -o -m1 'Cannot parse uuid' || echo 'malformed-uuid error: FAIL'
# Bad width.
${CLICKHOUSE_CLIENT} -q "select * from traceView('$trace_id', 0)" 2>&1 \
    | grep -o -m1 'timeline_width must be in' || echo 'bad-width error: FAIL'
# Unknown query_id: a clear error with the tracing hint.
${CLICKHOUSE_CLIENT} -q "select * from traceView(query_id = 'no-such-query-id-05043')" 2>&1 \
    | grep -o -m1 'No trace found for query_id' || echo 'missing-query-id error: FAIL'
# trace_id and query_id are mutually exclusive.
${CLICKHOUSE_CLIENT} -q "select * from traceView('$trace_id', query_id = 'x')" 2>&1 \
    | grep -o -m1 'exactly one of trace_id and query_id' || echo 'exclusivity error: FAIL'

${CLICKHOUSE_CLIENT} -q "drop table dist_over_two_shards_tv"
