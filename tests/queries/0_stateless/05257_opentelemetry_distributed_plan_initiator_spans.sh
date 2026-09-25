#!/usr/bin/env bash
# Tags: no-fasttest, no-old-analyzer, distributed
# no-old-analyzer: make_distributed_plan requires the analyzer.
#
# A distributed plan query (make_distributed_plan = 1) is a DAG of stages, each split into tasks.
# On the initiator, the execution of the plan is one span with the shape of the DAG in its
# attributes, and the dispatch of every task is a span under it:
#
#   query (initiator)
#   └── DistributedPlanExecutor::execute          clickhouse.distributed.{query_id,stages,tasks,stage_dependencies,...}
#       ├── DistributedPlanTask::dispatch [CLIENT] clickhouse.distributed.{task_id,stage,depends_on}, clickhouse.target_host
#       ├── DistributedPlanTask::dispatch [CLIENT]
#       └── ...
#
# With distributed_plan_execute_locally = 1 nothing is sent: the dispatch spans are INTERNAL and
# carry no target host. The execution span ends OK with the last stage and ERROR with the failure
# of a task. Only lower bounds are asserted: the number of stages and tasks is an implementation
# detail of the planner.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "drop table if exists t_dp_otel_dag"
${CLICKHOUSE_CLIENT} -q "create table t_dp_otel_dag (x UInt64) engine = MergeTree order by tuple()"
${CLICKHOUSE_CLIENT} -q "insert into t_dp_otel_dag select number % 10 from numbers(10000)"

# $1 - execute_locally, $2 - query id, $3 - trace id, $4 - the expression to aggregate.
function run_query
{
    local _execute_locally="$1"
    local _query_id="$2"
    local _trace_id="$3"
    local _expression="$4"
    ${CLICKHOUSE_CLIENT} \
        --opentelemetry-traceparent "00-$_trace_id-0000000000000073-01" \
        --query_id "$_query_id" \
        -q "select x, count($_expression) from t_dp_otel_dag group by x format Null
            settings make_distributed_plan = 1, enable_parallel_replicas = 0,
                distributed_plan_execute_locally = $_execute_locally,
                distributed_plan_fallback_to_local_execution = 0,
                distributed_plan_default_shuffle_join_bucket_count = 2,
                distributed_plan_default_reader_bucket_count = 2,
                distributed_plan_max_rows_to_broadcast = 0" 2>/dev/null
}

# The span ids of the `DistributedPlanExecutor::execute` spans of the trace, as a comma-separated
# list for an IN clause; `0` when there is none yet. Fetched separately: a scalar subquery would be
# evaluated differently in the distributed-plan test configuration.
function execution_span_ids
{
    local _trace_id="$1"
    local _ids
    _ids=$(${CLICKHOUSE_CLIENT} -q "
        with UUIDNumToString(toFixedString(unhex('$_trace_id'), 16)) as t
        select arrayStringConcat(groupArray(toString(span_id)), ', ')
        from system.opentelemetry_span_log
        where finish_date >= yesterday() and trace_id = t and operation_name = 'DistributedPlanExecutor::execute'")
    echo "${_ids:-0}"
}

# The spans of one trace, in the order asserted below:
# 1. `DistributedPlanExecutor::execute` spans with the DAG attributes and the given status;
# 2. `DistributedPlanTask::dispatch` spans of the given kind with the task attributes, an OK status
#    and (unless the tasks run locally) a target host, whose parent is an execution span;
# 3. `DistributedPlanTask::dispatch` spans of the trace that lack any of the above.
function span_counts_query
{
    local _trace_id="$1"
    local _query_id="$2"
    local _execute_locally="$3"
    local _execution_status="$4"
    local _dispatch_kind="$5"
    local _host_condition="attribute['clickhouse.target_host'] != ''"
    [[ "$_execute_locally" == 1 ]] && _host_condition="attribute['clickhouse.target_host'] = ''"
    echo "
        with UUIDNumToString(toFixedString(unhex('$_trace_id'), 16)) as t,
            operation_name = 'DistributedPlanTask::dispatch' and kind = '$_dispatch_kind'
                and attribute['clickhouse.distributed.task_id'] != ''
                and attribute['clickhouse.distributed.stage'] != ''
                and attribute['clickhouse.initial_query_id'] = '$_query_id'
                and mapContains(attribute, 'clickhouse.distributed.depends_on')
                and mapContains(attribute, 'clickhouse.exchange.kind')
                and $_host_condition
                and status_code = 'OK'
                and parent_span_id in ($(execution_span_ids "$_trace_id")) as good_dispatch
        select
            countIf(operation_name = 'DistributedPlanExecutor::execute' and kind = 'INTERNAL'
                and attribute['clickhouse.distributed.query_id'] != ''
                and attribute['clickhouse.initial_query_id'] = '$_query_id'
                and toUInt64OrZero(attribute['clickhouse.distributed.stages']) >= 1
                and toUInt64OrZero(attribute['clickhouse.distributed.tasks']) >= 1
                and attribute['clickhouse.distributed.stage_dependencies'] like '[%'
                and attribute['clickhouse.distributed.final_result_stream'] != ''
                and attribute['clickhouse.distributed.execute_locally'] = '$_execute_locally'
                and status_code = '$_execution_status'),
            countIf(good_dispatch),
            countIf(operation_name = 'DistributedPlanTask::dispatch' and not good_dispatch)
        from system.opentelemetry_span_log
        where finish_date >= yesterday() and trace_id = t
    "
}

# Every `DistributedPlanExecutor::execute` span of the trace must reach the initiator's `query` span
# through the parent_span_id chain. The depth of the chain depends on the thread pool, so the links
# are fetched once and walked here (a recursive CTE has no plan serialization, which breaks the
# distributed-plan test configuration). Spans are flushed by independent background threads, so a
# missing ancestor is a not-yet-flushed one; the caller retries.
function check_execution_spans_under_initiator
{
    local _trace_id="$1"
    local _query_id="$2"
    local _edges
    _edges=$(${CLICKHOUSE_CLIENT} -q "
        with UUIDNumToString(toFixedString(unhex('$_trace_id'), 16)) as t
        select span_id, parent_span_id,
            operation_name = 'query' and attribute['clickhouse.query_id'] = '$_query_id',
            operation_name = 'DistributedPlanExecutor::execute'
        from system.opentelemetry_span_log
        where finish_date >= yesterday() and trace_id = t")
    [[ -z "$_edges" ]] && return 1

    local -A _parent=()
    local _initiator_span="" _execution_spans=()
    local _s _p _is_initiator _is_execution
    while read -r _s _p _is_initiator _is_execution; do
        _parent[$_s]=$_p
        [[ "$_is_initiator" == 1 ]] && _initiator_span=$_s
        [[ "$_is_execution" == 1 ]] && _execution_spans+=("$_s")
    done <<< "$_edges"
    [[ -z "$_initiator_span" || ${#_execution_spans[@]} -eq 0 ]] && return 1

    local _cur _step _reached
    for _cur in "${_execution_spans[@]}"; do
        _reached=0
        for _step in {1..64}; do
            _cur=${_parent[$_cur]:-0}
            [[ "$_cur" == "0" ]] && break
            if [[ "$_cur" == "$_initiator_span" ]]; then
                _reached=1
                break
            fi
        done
        [[ $_reached -eq 1 ]] || return 1
    done
    return 0
}

# $1 - execute_locally, $2 - the expression to aggregate, $3 - expected status of the execution span,
# $4 - expected kind of the dispatch spans, $5 - expected minimum counts (see span_counts_query),
# $6 - label for the output.
function run_check
{
    local _execute_locally="$1"
    local _expression="$2"
    local _execution_status="$3"
    local _dispatch_kind="$4"
    local _expected
    read -ra _expected <<< "$5"
    local _label="$6"

    local _query_id="$CLICKHOUSE_TEST_UNIQUE_NAME-$_execute_locally-$_execution_status"
    local _trace_id
    _trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(generateUUIDv4()))")
    run_query "$_execute_locally" "$_query_id" "$_trace_id" "$_expression"

    local _counts=()
    local _counts_ok=0 _chain_ok=0
    for _retry in {1..30}; do
        ${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
        read -ra _counts <<< "$(${CLICKHOUSE_CLIENT} -q "$(span_counts_query "$_trace_id" "$_query_id" "$_execute_locally" "$_execution_status" "$_dispatch_kind")" | tr '\t' ' ')"
        _counts_ok=1
        for _i in "${!_expected[@]}"; do
            [[ "${_counts[$_i]:-0}" -ge "${_expected[$_i]}" ]] || _counts_ok=0
        done
        if [[ $_counts_ok -eq 1 ]] && check_execution_spans_under_initiator "$_trace_id" "$_query_id"; then
            _chain_ok=1
            break
        fi
        sleep 1
    done

    if [[ $_counts_ok -eq 1 ]]; then
        echo "$_label: execution and dispatch spans: OK"
    else
        echo "$_label: execution and dispatch spans: FAIL, counts: ${_counts[*]}, expected at least: ${_expected[*]}"
    fi
    if [[ $_chain_ok -eq 1 ]]; then
        echo "$_label: execution spans descend from the initiator query span: OK"
    else
        echo "$_label: execution spans descend from the initiator query span: FAIL"
    fi
    # Every dispatch span of the trace must be a well-formed child of the execution span: exact.
    echo "$_label: dispatch spans outside the execution span: ${_counts[2]:-?}"
}

run_check 0 "" "OK" "CLIENT" "1 1" "stateless workers"
run_check 1 "" "OK" "INTERNAL" "1 1" "local execution"
# A task fails: the execution span records the failure. The dispatch of the failing task itself
# succeeded (the worker accepted it), so the dispatch spans are still OK.
run_check 0 "throwIf(x = 5, 'injected task failure')" "ERROR" "CLIENT" "1 1" "failing task"

${CLICKHOUSE_CLIENT} -q "drop table t_dp_otel_dag"
