#!/usr/bin/env bash
# Tags: distributed
# Test writing telemetry context back to ClientInfo

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

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

query_id="$CLICKHOUSE_TEST_UNIQUE_NAME-sampled"

${CLICKHOUSE_CLIENT} \
    --opentelemetry_start_trace_probability=1 \
    --query_id "$query_id" \
    --query "select * from remote('127.0.0.2', system, one) format Null"

# The trace id is taken from the initiator's 'query' span (only the initiator has this
# query_id; the secondary query on 127.0.0.2 has its own). The secondary query must appear
# in the same trace: at least two 'query' spans, and at least one of them from a query
# whose query_id differs from the initiator's.
counts_query="
    with (select any(trace_id) from system.opentelemetry_span_log
          where finish_date >= yesterday() and operation_name = 'query'
            and attribute['clickhouse.query_id'] = '$query_id') as t
    select
        countIf(operation_name = 'query'),
        countIf(operation_name = 'query' and attribute['clickhouse.query_id'] != '$query_id')
    from system.opentelemetry_span_log
    where finish_date >= yesterday() and trace_id = t"

poll_spans "$counts_query" "2 1" || exit 1
echo "sampled trace joins remote query: OK"
