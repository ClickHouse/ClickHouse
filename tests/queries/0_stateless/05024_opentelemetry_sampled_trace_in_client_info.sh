#!/usr/bin/env bash
# Tags: distributed
# Test writing telemetry context back to ClientInfo

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query_id="$CLICKHOUSE_TEST_UNIQUE_NAME-sampled"

${CLICKHOUSE_CLIENT} \
    --opentelemetry_start_trace_probability=1 \
    --query_id "$query_id" \
    --query "select * from remote('127.0.0.2', system, one) format Null"

# Find the initiator's 'query' span by query_id and require that the remote rewritten
# SELECT hangs under it through the parent_span_id chain. The chain depth varies with
# the connection paths (pipeline spans, the read context fiber span, 'Connection::sendQuery()'),
# so fetch the trace's parent links with a plain SELECT and walk them here: a recursive
# CTE cannot be used for a depth-independent walk because ReadFromRecursiveCTEStep has
# no plan serialization, which breaks the distributed-plan and parallel-replicas
# configurations. The check targets the remote SELECT because it is sent from a
# connection thread, where only the ClientInfo write-back can parent it correctly
# (unlike the remote DESC TABLE, which the query thread's ambient context parents anyway).
function check_remote_select_under_initiator
{
    # One snapshot of the initiator's trace: parent links plus role markers
    # (the initiator 'query' span, the remote SELECT 'query' spans), all from
    # the same consistent read.
    local _edges
    _edges=$(${CLICKHOUSE_CLIENT} -q "
        with (select any((trace_id, span_id)) from system.opentelemetry_span_log
              where finish_date >= yesterday() and operation_name = 'query'
                  and attribute['clickhouse.query_id'] = '$query_id') as initiator
        select span_id, parent_span_id,
            span_id = initiator.2,
            operation_name = 'query'
                and attribute['clickhouse.query_id'] != '$query_id'
                and attribute['db.statement'] like 'SELECT%'
        from system.opentelemetry_span_log
        where finish_date >= yesterday() and trace_id = initiator.1
        settings enable_analyzer = 1")
    [[ -z "$_edges" ]] && return 1

    local -A _parent=()
    local _initiator_span="" _remote_spans=()
    local _s _p _is_initiator _is_remote_select
    while read -r _s _p _is_initiator _is_remote_select; do
        _parent[$_s]=$_p
        [[ "$_is_initiator" == 1 ]] && _initiator_span=$_s
        [[ "$_is_remote_select" == 1 ]] && _remote_spans+=("$_s")
    done <<< "$_edges"
    [[ -z "$_initiator_span" || ${#_remote_spans[@]} -eq 0 ]] && return 1

    # Walk up the parent chain from each remote SELECT 'query' span. Intermediate spans
    # are flushed by independent background threads, so a not-yet-flushed ancestor breaks
    # the walk; the caller retries.
    local _cur _step
    for _cur in "${_remote_spans[@]}"; do
        for _step in {1..64}; do
            _cur=${_parent[$_cur]:-0}
            [[ "$_cur" == "$_initiator_span" ]] && return 0
            [[ "$_cur" == "0" ]] && break
        done
    done
    return 1
}

for _retry in {1..20}; do
    ${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
    if check_remote_select_under_initiator; then
        echo "remote query span attached under initiator query span: OK"
        exit 0
    fi
    sleep 1
done
echo "remote SELECT query span not under the initiator query span within 20s" >&2
exit 1
