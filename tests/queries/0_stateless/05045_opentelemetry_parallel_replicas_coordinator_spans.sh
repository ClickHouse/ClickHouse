#!/usr/bin/env bash
# Tags: distributed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query with parallel replicas produces, in one trace:
#   - one `ParallelReplicasReadingCoordinator` summary span emitted when the coordinator is
#     destroyed, with the totals of the coordination (`clickhouse.replicas_count`,
#     `clickhouse.replicas_used`, `clickhouse.requests`, `clickhouse.marks_assigned`, ...);
#   - a `ParallelReplicasReadingCoordinator::handleInitialAllRangesAnnouncement` span per
#     announcement and a `ParallelReplicasReadingCoordinator::handleRequest` span per read
#     request served, each carrying `clickhouse.replica_num`;
#   - per-replica `RemoteQueryExecutor::execute` fragment spans with `clickhouse.replica_num`
#     and `clickhouse.cluster`;
#   - replica-side `ParallelReplicasAnnouncement`/`ParallelReplicasReadRequest` spans with
#     `clickhouse.replica_num` and `clickhouse.replicas_count`.
# Only presence and lower bounds are asserted: how many replicas participate, announce and
# request work before the reading completes depends on scheduling (the initiator may finish
# and tear down the connections while announcements of slower replicas are still in flight).
# The `parallel_replicas_wait_for_unused_replicas` failpoint disables the early cancellation
# of replicas that did not get work, so that the remote replicas reliably at least start.

CLUSTER="test_cluster_one_shard_three_replicas_localhost"
TABLE="t_pr_coordinator_spans"

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

${CLICKHOUSE_CLIENT} -q "drop table if exists $TABLE"
# Enough marks that the coordinator serves several read requests.
${CLICKHOUSE_CLIENT} -q "
    create table $TABLE (k UInt64, v String)
    engine = MergeTree order by k
    as select number, toString(number) from numbers(1000000)
"

# Deliberately not disabled at the end: other tests enable this failpoint without disabling
# it, so disabling would race against them when tests run in parallel.
${CLICKHOUSE_CLIENT} -q "system enable failpoint parallel_replicas_wait_for_unused_replicas"

# Both asynchronous (fiber) and fully synchronous connection handling: the per-replica
# fragment spans are produced by different mechanisms on the two paths.
for async_socket in 1 0; do
    echo "=== async_socket_for_remote=$async_socket ==="

    trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
    query_id="$CLICKHOUSE_TEST_UNIQUE_NAME-$async_socket"

    # automatic_parallel_replicas_mode=0: mode 2 (randomized by the test harness) only collects
    # statistics and never actually executes with parallel replicas.
    ${CLICKHOUSE_CLIENT} \
        --opentelemetry-traceparent "00-$trace_id-0000000000000073-01" \
        --enable_parallel_replicas=2 \
        --automatic_parallel_replicas_mode=0 \
        --max_parallel_replicas=3 \
        --cluster_for_parallel_replicas="$CLUSTER" \
        --parallel_replicas_for_non_replicated_merge_tree=1 \
        --async_socket_for_remote="$async_socket" \
        --query_id "$query_id" \
        --query "select sum(k) from $TABLE format Null"

    # The summary span is emitted when the coordinator is destroyed, which can lag the query
    # end; poll for it together with the rest.
    poll_spans "
        with UUIDNumToString(toFixedString(unhex('$trace_id'), 16)) as t
        select
            countIf(operation_name = 'ParallelReplicasReadingCoordinator'),
            countIf(operation_name = 'ParallelReplicasReadingCoordinator::handleInitialAllRangesAnnouncement'),
            countIf(operation_name = 'ParallelReplicasReadingCoordinator::handleRequest'),
            countIf(operation_name = 'RemoteQueryExecutor::execute' and attribute['clickhouse.replica_num'] != ''),
            countIf(operation_name = 'ParallelReplicasAnnouncement'),
            countIf(operation_name = 'ParallelReplicasReadRequest')
        from system.opentelemetry_span_log
        where finish_date >= yesterday() and trace_id = t" "1 1 1 1 1 1" \
    || exit 1

    ${CLICKHOUSE_CLIENT} -q "
        with UUIDNumToString(toFixedString(unhex('$trace_id'), 16)) as t
        select
            if(countIf(operation_name = 'ParallelReplicasReadingCoordinator'
                       and attribute['clickhouse.replicas_count'] = '3'
                       and toUInt64OrZero(attribute['clickhouse.replicas_used']) >= 1
                       and toUInt64OrZero(attribute['clickhouse.requests']) >= 1
                       and toUInt64OrZero(attribute['clickhouse.marks_assigned']) >= 1
                       and attribute['clickhouse.reading_completed'] = '1') = 1,
               'coordinator summary span: OK', 'coordinator summary span: FAIL'),
            if(countIf(operation_name = 'ParallelReplicasReadingCoordinator::handleInitialAllRangesAnnouncement'
                       and attribute['clickhouse.replica_num'] in ('0', '1', '2')
                       and attribute['clickhouse.mode'] != ''
                       and toUInt64OrZero(attribute['clickhouse.marks']) >= 1) >= 1,
               'announcement handler spans: OK', 'announcement handler spans: FAIL'),
            if(countIf(operation_name = 'ParallelReplicasReadingCoordinator::handleRequest'
                       and attribute['clickhouse.replica_num'] in ('0', '1', '2')
                       and attribute['clickhouse.finish'] != '') >= 1
                   and countIf(operation_name = 'ParallelReplicasReadingCoordinator::handleRequest'
                       and toUInt64OrZero(attribute['clickhouse.marks_assigned']) >= 1) >= 1,
               'read request handler spans: OK', 'read request handler spans: FAIL'),
            if(countIf(operation_name = 'RemoteQueryExecutor::execute'
                       and attribute['clickhouse.replica_num'] in ('0', '1', '2')
                       and attribute['clickhouse.cluster'] = '$CLUSTER'
                       and attribute['clickhouse.initial_query_id'] = '$query_id') >= 1,
               'per-replica fragment spans: OK', 'per-replica fragment spans: FAIL'),
            if(countIf(operation_name = 'ParallelReplicasAnnouncement'
                       and attribute['clickhouse.replica_num'] in ('0', '1', '2')
                       and attribute['clickhouse.replicas_count'] = '3') >= 1
                   and countIf(operation_name = 'ParallelReplicasReadRequest'
                       and attribute['clickhouse.replica_num'] in ('0', '1', '2')) >= 1,
               'replica-side spans: OK', 'replica-side spans: FAIL')
        from system.opentelemetry_span_log
        where finish_date >= yesterday() and trace_id = t
        format TSV
    "
done

${CLICKHOUSE_CLIENT} -q "drop table $TABLE"
