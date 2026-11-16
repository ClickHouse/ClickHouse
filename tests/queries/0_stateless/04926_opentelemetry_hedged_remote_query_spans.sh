#!/usr/bin/env bash
# Tags: distributed, no-darwin
# no-darwin: hedged requests depend on epoll and are compiled only on Linux
# (HedgedConnections, ConnectionEstablisherAsync and PacketReceiver are guarded by
# OS_LINUX); on other systems use_hedged_requests silently falls back to
# MultiplexedConnections and the asserted spans can never appear.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# ConnectionEstablisherAsync and PacketReceiver are constructed at different points of the
# query lifecycle than RemoteQueryExecutorReadContext (preallocated by
# HedgedConnectionsFactory / created on the hedged read path), so their tracing context
# capture is asserted separately from 04869_opentelemetry_async_remote_query_spans. Both
# fibers run even for pooled connections, so no fresh connection needs to be forced.

function poll_spans
{
    # Spans are flushed to the log by background threads, poll until the expected
    # counts are reached. $1 - SQL returning several counts, $2 - expected minimums
    # (space-separated, compared field by field).
    local _query="$1"
    local _expected
    read -ra _expected <<< "$2"
    local _counts=()
    for _retry in {1..20}; do
        ${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
        read -ra _counts <<< "$(${CLICKHOUSE_CLIENT} -q "$_query" | tr '\t' ' ')"
        local _ok=1
        for _i in "${!_expected[@]}"; do
            [[ "${_counts[$_i]:-0}" -ge "${_expected[$_i]}" ]] || _ok=0
        done
        [[ $_ok -eq 1 ]] && return 0
        sleep 1
    done
    echo "spans did not appear in time, last counts: ${_counts[*]}, expected: ${_expected[*]}" >&2
    return 1
}

trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")

${CLICKHOUSE_CLIENT} \
    --opentelemetry-traceparent "00-$trace_id-0000000000000073-01" \
    --use_hedged_requests=1 \
    --async_socket_for_remote=1 \
    --query "select * from remote('127.0.0.2', system, one) format Null"

poll_spans "
    with UUIDNumToString(toFixedString(unhex('$trace_id'), 16)) as t
    select
        countIf(operation_name = 'ConnectionEstablisherAsync' and parent_span_id != 0),
        countIf(operation_name = 'PacketReceiver' and parent_span_id != 0)
    from system.opentelemetry_span_log
    where finish_date >= yesterday() and trace_id = t" "1 1" \
|| exit 1
echo "hedged ConnectionEstablisherAsync span: OK"
echo "hedged PacketReceiver span: OK"
