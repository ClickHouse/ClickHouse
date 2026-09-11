#!/usr/bin/env bash
# Tags: no-fasttest, no-old-analyzer, distributed
# no-old-analyzer: make_distributed_plan requires the analyzer.
#
# A distributed plan query (make_distributed_plan = 1) dispatches its tasks to stateless workers
# over HTTP. The trace context travels with the dispatch request as W3C `traceparent` /
# `tracestate` headers, so the worker's task span joins the initiator's trace:
#
#   query (initiator)
#   └── StatelessWorkerClient::sendTask [CLIENT]
#       └── InterserverIOHTTPHandler [SERVER]
#           └── (worker thread pool span)
#               └── DistributedPlanTask::execute [SERVER]
#
# The task span outlives the request span by design: the start request returns as soon as the task
# is scheduled and the initiator polls for completion. With distributed_plan_execute_locally = 1 the
# task runs on an initiator thread and its span attaches to the initiator's trace directly.
# The harness worker cluster (test_cluster_one_shard_two_replicas) is this same server, so every
# span lands in the local span log. Only lower bounds are asserted: the number of tasks is an
# implementation detail of the planner.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "drop table if exists t_dp_otel"
${CLICKHOUSE_CLIENT} -q "create table t_dp_otel (x UInt64) engine = MergeTree order by tuple()"
${CLICKHOUSE_CLIENT} -q "insert into t_dp_otel select number % 10 from numbers(10000)"

function run_query
{
    local _execute_locally="$1"
    local _query_id="$2"
    local _trace_id="$3"
    ${CLICKHOUSE_CLIENT} \
        --opentelemetry-traceparent "00-$_trace_id-0000000000000073-01" \
        --query_id "$_query_id" \
        -q "select x, count() from t_dp_otel group by x format Null
            settings make_distributed_plan = 1, enable_parallel_replicas = 0,
                distributed_plan_execute_locally = $_execute_locally,
                distributed_plan_default_shuffle_join_bucket_count = 2,
                distributed_plan_default_reader_bucket_count = 2,
                distributed_plan_max_rows_to_broadcast = 0"
}

# Counts of the spans of one trace, in the order asserted below:
# 1. `StatelessWorkerClient::sendTask` CLIENT spans with the dispatch attributes;
# 2. `InterserverIOHTTPHandler` SERVER spans (the worker side of the dispatch request);
# 3. `DistributedPlanTask::execute` SERVER spans with the task attributes and a finished status.
function span_counts_query
{
    local _trace_id="$1"
    local _query_id="$2"
    local _execute_locally="$3"
    echo "
        with UUIDNumToString(toFixedString(unhex('$_trace_id'), 16)) as t
        select
            countIf(operation_name = 'StatelessWorkerClient::sendTask' and kind = 'CLIENT'
                and attribute['clickhouse.distributed.task_id'] != ''
                and attribute['clickhouse.initial_query_id'] = '$_query_id'
                and attribute['clickhouse.target_host'] != ''),
            countIf(operation_name = 'InterserverIOHTTPHandler' and kind = 'SERVER'),
            countIf(operation_name = 'DistributedPlanTask::execute' and kind = 'SERVER'
                and attribute['clickhouse.distributed.task_id'] != ''
                and attribute['clickhouse.initial_query_id'] = '$_query_id'
                and attribute['clickhouse.distributed.plan_hash'] != ''
                and attribute['clickhouse.distributed.execute_locally'] = '$_execute_locally'
                and attribute['clickhouse.exchange.outputs'] != ''
                and attribute['clickhouse.query_id'] != ''
                and attribute['clickhouse.query_status'] = 'QueryFinish')
        from system.opentelemetry_span_log
        where finish_date >= yesterday() and trace_id = t
    "
}

# Every `DistributedPlanTask::execute` span of the trace must reach the initiator's `query` span
# through the parent_span_id chain, passing through a span of type $3 on the way (empty = no
# requirement). The depth of the chain depends on the thread pool and connection paths, so the
# links are fetched once and walked here (a recursive CTE has no plan serialization, which breaks
# the distributed-plan test configuration). Spans are flushed by independent background threads,
# so a missing ancestor is a not-yet-flushed one; the caller retries.
function check_task_spans_under_initiator
{
    local _trace_id="$1"
    local _query_id="$2"
    local _via="$3"
    local _edges
    _edges=$(${CLICKHOUSE_CLIENT} -q "
        with UUIDNumToString(toFixedString(unhex('$_trace_id'), 16)) as t
        select span_id, parent_span_id,
            operation_name = 'query' and attribute['clickhouse.query_id'] = '$_query_id',
            operation_name = 'DistributedPlanTask::execute',
            operation_name = '$_via'
        from system.opentelemetry_span_log
        where finish_date >= yesterday() and trace_id = t")
    [[ -z "$_edges" ]] && return 1

    local -A _parent=() _is_via=()
    local _initiator_span="" _task_spans=()
    local _s _p _is_initiator _is_task _via_flag
    while read -r _s _p _is_initiator _is_task _via_flag; do
        _parent[$_s]=$_p
        _is_via[$_s]=$_via_flag
        [[ "$_is_initiator" == 1 ]] && _initiator_span=$_s
        [[ "$_is_task" == 1 ]] && _task_spans+=("$_s")
    done <<< "$_edges"
    [[ -z "$_initiator_span" || ${#_task_spans[@]} -eq 0 ]] && return 1

    local _cur _step _via_seen _reached
    for _cur in "${_task_spans[@]}"; do
        _via_seen=0
        _reached=0
        for _step in {1..64}; do
            _cur=${_parent[$_cur]:-0}
            [[ "$_cur" == "0" ]] && break
            [[ "${_is_via[$_cur]:-0}" == 1 ]] && _via_seen=1
            if [[ "$_cur" == "$_initiator_span" ]]; then
                _reached=1
                break
            fi
        done
        [[ $_reached -eq 1 ]] || return 1
        [[ -z "$_via" || $_via_seen -eq 1 ]] || return 1
    done
    return 0
}

# $1 - execute_locally, $2 - expected minimum counts (space-separated, see span_counts_query),
# $3 - span type every task span must descend through, $4 - label for the output.
function run_check
{
    local _execute_locally="$1"
    local _expected
    read -ra _expected <<< "$2"
    local _via="$3"
    local _label="$4"

    local _query_id="$CLICKHOUSE_TEST_UNIQUE_NAME-$_execute_locally"
    local _trace_id
    _trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))")
    run_query "$_execute_locally" "$_query_id" "$_trace_id"

    local _counts=()
    local _counts_ok=0 _chain_ok=0
    for _retry in {1..30}; do
        ${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
        read -ra _counts <<< "$(${CLICKHOUSE_CLIENT} -q "$(span_counts_query "$_trace_id" "$_query_id" "$_execute_locally")" | tr '\t' ' ')"
        _counts_ok=1
        for _i in "${!_expected[@]}"; do
            [[ "${_counts[$_i]:-0}" -ge "${_expected[$_i]}" ]] || _counts_ok=0
        done
        if [[ $_counts_ok -eq 1 ]] && check_task_spans_under_initiator "$_trace_id" "$_query_id" "$_via"; then
            _chain_ok=1
            break
        fi
        sleep 1
    done

    if [[ $_counts_ok -eq 1 ]]; then
        echo "$_label: dispatch, request and task spans: OK"
    else
        echo "$_label: dispatch, request and task spans: FAIL, counts: ${_counts[*]}, expected at least: ${_expected[*]}"
    fi
    if [[ $_chain_ok -eq 1 ]]; then
        echo "$_label: task spans descend from the initiator query span: OK"
    else
        echo "$_label: task spans descend from the initiator query span: FAIL"
    fi

    if [[ "$_execute_locally" == 1 ]]; then
        # Nothing is dispatched over HTTP when the tasks run in-process, so this is exact.
        echo "$_label: dispatch spans: ${_counts[0]:-?}"
    fi
}

run_check 0 "1 1 1" "StatelessWorkerClient::sendTask" "stateless workers"
run_check 1 "0 0 1" "" "local execution"

${CLICKHOUSE_CLIENT} -q "drop table t_dp_otel"
