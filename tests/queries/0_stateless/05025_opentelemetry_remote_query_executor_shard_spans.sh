#!/usr/bin/env bash
# Tags: distributed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every remote read of a distributed SELECT is covered by exactly one per-shard
# `RemoteQueryExecutor::execute` span: the read context fiber on the asynchronous
# paths, a span kept alive by the executor itself on the fully synchronous path.
# When the query is sent synchronously but read asynchronously
# (async_socket_for_remote = 1, async_query_sending_for_remote = 0), the span opened
# at send time is handed over to the fiber, which continues it - the shard must not
# get a separate send-side and read-side span. The spans carry
# attributes identifying the fragment: `clickhouse.cluster`, `clickhouse.shard_num`,
# `clickhouse.processed_stage`, `clickhouse.query_id`, `clickhouse.initial_query_id`
# and, once the connections are established, `clickhouse.target_host`.

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

function fragment_counts_query
{
    local _trace_id="$1"
    local _query_id="$2"
    echo "
        with UUIDNumToString(toFixedString(unhex('$_trace_id'), 16)) as t
        select
            countIf(attribute['clickhouse.cluster'] = 'test_cluster_two_shards'),
            uniqExactIf(attribute['clickhouse.shard_num'], attribute['clickhouse.shard_num'] in ('1', '2')),
            countIf(attribute['clickhouse.initial_query_id'] = '$_query_id'),
            countIf(attribute['clickhouse.target_host'] != ''),
            countIf(attribute['clickhouse.processed_stage'] != '')
        from system.opentelemetry_span_log
        where finish_date >= yesterday() and trace_id = t
          and operation_name = 'RemoteQueryExecutor::execute'
    "
}

function assert_fragment_spans
{
    local _trace_id="$1"
    local _query_id="$2"
    ${CLICKHOUSE_CLIENT} -q "
        with UUIDNumToString(toFixedString(unhex('$_trace_id'), 16)) as t
        select
            if(countIf(attribute['clickhouse.cluster'] = 'test_cluster_two_shards') = 2
                   and uniqExactIf(attribute['clickhouse.shard_num'], attribute['clickhouse.shard_num'] in ('1', '2')) = 2,
               'exactly one fragment span per shard: OK',
               'exactly one fragment span per shard: FAIL, ' || toString(countIf(attribute['clickhouse.cluster'] = 'test_cluster_two_shards')) || ' spans'),
            if(countIf(attribute['clickhouse.initial_query_id'] = '$_query_id'
                   and attribute['clickhouse.query_id'] != ''
                   and attribute['clickhouse.processed_stage'] != '') = 2
                   and countIf(attribute['clickhouse.target_host'] != '') = 2,
               'fragment span attributes: OK', 'fragment span attributes: FAIL')
        from system.opentelemetry_span_log
        where finish_date >= yesterday() and trace_id = t
          and operation_name = 'RemoteQueryExecutor::execute'
        format TSV
    "
}

${CLICKHOUSE_CLIENT} -q "drop table if exists dist_over_two_shards"
${CLICKHOUSE_CLIENT} -q "
    create table dist_over_two_shards (dummy UInt8)
    engine = Distributed(test_cluster_two_shards, system, one)
"

# (async_socket_for_remote, async_query_sending_for_remote): asynchronous reading with
# asynchronous and synchronous sending, and the fully synchronous path.
# async_query_sending_for_remote has no effect without async_socket_for_remote.
for async_settings in "1 1" "1 0" "0 0"; do
    read -r async_socket async_send <<< "$async_settings"
    echo "=== async_socket_for_remote=$async_socket async_query_sending_for_remote=$async_send ==="

    trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
    query_id="$CLICKHOUSE_TEST_UNIQUE_NAME-$async_socket-$async_send"

    # prefer_localhost_replica=0: both shards of the cluster must be read through
    # RemoteQueryExecutor, otherwise the first shard is read by a local plan.
    ${CLICKHOUSE_CLIENT} \
        --opentelemetry-traceparent "00-$trace_id-0000000000000073-01" \
        --async_socket_for_remote="$async_socket" \
        --async_query_sending_for_remote="$async_send" \
        --prefer_localhost_replica=0 \
        --query_id "$query_id" \
        --query "select * from dist_over_two_shards format Null"

    poll_spans "$(fragment_counts_query "$trace_id" "$query_id")" "2 2 2 2 2" || exit 1

    assert_fragment_spans "$trace_id" "$query_id"
done

# The *Cluster table functions (urlCluster, s3Cluster, ...) do not go through
# ReadFromRemote: their RemoteQueryExecutors are wired in IStorageCluster::readFromCluster,
# so the fragment attributes must be asserted on that path separately. (The cluster()
# function is a remote() variant over StorageDistributed and would not cover it.)
# urlCluster over test_cluster_two_shards loops back to this server over HTTP; the cluster
# of a *Cluster function uses every replica as a shard, and with two single-replica shards
# the spans carry the same shard_num values 1 and 2.
echo "=== urlCluster (IStorageCluster path) ==="

trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
url_query_id="$CLICKHOUSE_TEST_UNIQUE_NAME-url"

${CLICKHOUSE_CLIENT} \
    --opentelemetry-traceparent "00-$trace_id-0000000000000073-01" \
    --query_id "$url_query_id" \
    --query "select * from urlCluster('test_cluster_two_shards', 'http://localhost:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+1', 'TSV', 'x UInt8') format Null"

poll_spans "$(fragment_counts_query "$trace_id" "$url_query_id")" "2 2 2 2 2" || exit 1

assert_fragment_spans "$trace_id" "$url_query_id"

# The synchronous path has no fiber: the span is kept alive by the executor itself and
# finished on EndOfStream. It must cover the whole remote read, not only connection
# establishing and query sending: with a remote sleep(1) the span must last at least
# one second. (Only a lower bound is asserted, so the check cannot flake under load.)
echo "=== synchronous span covers the remote read ==="

trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
sync_query_id="$CLICKHOUSE_TEST_UNIQUE_NAME-sync"

${CLICKHOUSE_CLIENT} \
    --opentelemetry-traceparent "00-$trace_id-0000000000000073-01" \
    --async_socket_for_remote=0 \
    --prefer_localhost_replica=0 \
    --query_id "$sync_query_id" \
    --query "select * from remote('127.0.0.2', view(select sleep(1) from system.one)) format Null"

poll_spans "
    with UUIDNumToString(toFixedString(unhex('$trace_id'), 16)) as t
    select count()
    from system.opentelemetry_span_log
    where finish_date >= yesterday() and trace_id = t
      and operation_name = 'RemoteQueryExecutor::execute'
      and attribute['clickhouse.initial_query_id'] = '$sync_query_id'" "1" \
|| exit 1

# max, not min: the query also produces a short span for the auxiliary structure
# inference query of remote() over a view; the span of the data read must cover the sleep.
${CLICKHOUSE_CLIENT} -q "
    with UUIDNumToString(toFixedString(unhex('$trace_id'), 16)) as t
    select if(max(finish_time_us - start_time_us) >= 1000000,
              'sync span covers the remote sleep: OK',
              'sync span covers the remote sleep: FAIL, lasted ' || toString(max(finish_time_us - start_time_us)) || ' us')
    from system.opentelemetry_span_log
    where finish_date >= yesterday() and trace_id = t
      and operation_name = 'RemoteQueryExecutor::execute'
      and attribute['clickhouse.initial_query_id'] = '$sync_query_id'
    format TSV
"

# Cancellation: killing the query destroys the suspended fiber and unwinds its stack.
# The attributes buffered inside the fiber (`clickhouse.target_host` is added after the
# connections are established) must still reach the span: they are copied by a scope
# guard that runs before the span is finished on every exit from the fiber routine,
# including this forced unwind.
echo "=== cancellation ==="

trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
kill_query_id="$CLICKHOUSE_TEST_UNIQUE_NAME-kill"

${CLICKHOUSE_CLIENT} \
    --opentelemetry-traceparent "00-$trace_id-0000000000000073-01" \
    --async_socket_for_remote=1 \
    --async_query_sending_for_remote=1 \
    --query_id "$kill_query_id" \
    --function_sleep_max_microseconds_per_block=10000000 \
    --query "select * from remote('127.0.0.2', view(select sleep(3) from system.one)) format Null" \
    >/dev/null 2>&1 &

# Wait until the remote leg is in flight before killing: the non-initial entry appears in
# system.processes (127.0.0.2 loops back to this same server) only after the query was
# sent, i.e. after the connections were established and the target_host attribute was
# buffered inside the fiber.
for _retry in {1..100}; do
    started=$(${CLICKHOUSE_CLIENT} -q "select count() from system.processes where initial_query_id = '$kill_query_id' and query_id != initial_query_id")
    [[ "$started" -ge 1 ]] && break
    sleep 0.1
done
${CLICKHOUSE_CLIENT} -q "kill query where query_id = '$kill_query_id' sync format Null"
wait

poll_spans "
    with UUIDNumToString(toFixedString(unhex('$trace_id'), 16)) as t
    select countIf(operation_name = 'RemoteQueryExecutor::execute'
                   and attribute['clickhouse.target_host'] != ''
                   and attribute['clickhouse.initial_query_id'] = '$kill_query_id')
    from system.opentelemetry_span_log
    where finish_date >= yesterday() and trace_id = t" "1" \
|| exit 1
echo "buffered attributes flushed on cancellation: OK"
