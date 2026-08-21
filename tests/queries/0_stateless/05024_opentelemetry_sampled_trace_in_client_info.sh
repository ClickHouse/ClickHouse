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

# Find the initiator's 'query' span by query_id, walk parent_span_id links down from it,
# and require that the remote rewritten SELECT is among the descendants. The check
# targets the remote SELECT because it is sent from a connection thread with no ambient
# trace context, so only the ClientInfo write-back can parent it correctly (unlike the
# remote DESC TABLE, which the query thread's ambient context parents anyway).
counts_query="
    with recursive initiator_descendants as
        (
            select span_id
            from system.opentelemetry_span_log
            where finish_date >= yesterday() and operation_name = 'query'
                and attribute['clickhouse.query_id'] = '$query_id'
            union all
            select l.span_id
            from system.opentelemetry_span_log as l
            inner join initiator_descendants as d on l.parent_span_id = d.span_id
            where l.finish_date >= yesterday()
        )
    select
        countIf(operation_name = 'query'),
        countIf(operation_name = 'query'
            and attribute['clickhouse.query_id'] != '$query_id'
            and attribute['db.statement'] like 'SELECT%'
            and span_id in (select span_id from initiator_descendants))
    from system.opentelemetry_span_log
    where finish_date >= yesterday()
        and trace_id = (select any(trace_id) from system.opentelemetry_span_log
                        where finish_date >= yesterday() and operation_name = 'query'
                            and attribute['clickhouse.query_id'] = '$query_id')
    settings enable_analyzer = 1"

poll_spans "$counts_query" "2 1" || exit 1
echo "remote query span attached under initiator query span: OK"
