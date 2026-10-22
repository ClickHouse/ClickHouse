#!/usr/bin/env bash
# Tags: distributed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `skip_unavailable_shards` lets the query succeed while a shard fragment fails. The fragment
# span (`RemoteQueryExecutor::execute`) must still record the failure: status ERROR with the
# failure text, tagged with the `clickhouse.shard_skipped` attribute so monitoring can tell
# tolerated failures apart from fatal ones. Covers the no-connection skip in sendQueryUnlocked
# (async and sync paths) and the ignored remote exception packet in processPacket.

function check_skipped_shard_span
{
    local _trace_id="$1"
    local _message_pattern="$2"
    local _label="$3"
    local _expected="${4:-1}"
    local _query="
        with UUIDNumToString(toFixedString(unhex('$_trace_id'), 16)) as t
        select countIf(status_code = 'ERROR'
                       and status_message like '$_message_pattern'
                       and attribute['clickhouse.shard_skipped'] = '1')
        from system.opentelemetry_span_log
        where finish_date >= yesterday() and trace_id = t
          and operation_name = 'RemoteQueryExecutor::execute'"
    # Spans are flushed to the log by background threads, poll until the span appears.
    for _retry in {1..20}; do
        ${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
        if [[ "$(${CLICKHOUSE_CLIENT} -q "$_query")" -ge "$_expected" ]]; then
            echo "$_label: OK"
            return 0
        fi
        sleep 1
    done
    echo "$_label: FAIL"
    return 1
}

# No-connection skip on the asynchronous path: the second shard of `test_unavailable_shard`
# is a dead port, so the pool yields zero connections and the shard is skipped. The query
# itself must succeed (prints the live shard's count).
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
${CLICKHOUSE_CLIENT} \
    --opentelemetry-traceparent "00-$trace_id-0000000000000073-01" \
    --skip_unavailable_shards=1 \
    --use_hedged_requests=0 \
    --query "select count() from cluster(test_unavailable_shard, system.one)" 2>/dev/null
check_skipped_shard_span "$trace_id" "%skip_unavailable_shards%" \
    "no-connection skip on async path marks span ERROR" || exit 1

# The same no-connection skip on the fully synchronous path, where the fragment span is the
# detached synchronous-path span.
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
${CLICKHOUSE_CLIENT} \
    --opentelemetry-traceparent "00-$trace_id-0000000000000073-01" \
    --skip_unavailable_shards=1 \
    --async_socket_for_remote=0 \
    --async_query_sending_for_remote=0 \
    --use_hedged_requests=0 \
    --query "select count() from cluster(test_unavailable_shard, system.one)" 2>/dev/null
check_skipped_shard_span "$trace_id" "%skip_unavailable_shards%" \
    "no-connection skip on sync path marks span ERROR" || exit 1

# Ignored remote exception packet: the shard terminates the query with an exception before
# producing any data, and `skip_unavailable_shards_mode` tolerates it. The only shard was skipped
# without returning data, so the query fails with `ALL_CONNECTION_TRIES_FAILED`, and the span must
# still carry the remote exception text rather than that failure.
# `throwIf` must feed the aggregate: a `count()` over it never evaluates the column at all.
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
${CLICKHOUSE_CLIENT} \
    --opentelemetry-traceparent "00-$trace_id-0000000000000073-01" \
    --skip_unavailable_shards=1 \
    --skip_unavailable_shards_mode='unavailable_or_exception_before_processing' \
    --use_hedged_requests=0 \
    --query "select sum(throwIf(1)) from remote('127.0.0.2', system.one)" 2>&1 \
    | grep -o -m1 'ALL_CONNECTION_TRIES_FAILED'
check_skipped_shard_span "$trace_id" "%throwIf%" \
    "ignored remote exception marks span ERROR" || exit 1

# Every shard of a three-shard cluster skipped without returning data. The query fails with
# `ALL_CONNECTION_TRIES_FAILED`, and the shard whose skip raises it is recorded like its two siblings
# instead of carrying that failure: all three spans, not two. Checked on both execution paths.
${CLICKHOUSE_CLIENT} -q "drop table if exists dist_05055_all_dead"
trap '${CLICKHOUSE_CLIENT} -q "drop table if exists dist_05055_all_dead"' EXIT
${CLICKHOUSE_CLIENT} -q "
    create table dist_05055_all_dead (dummy UInt8)
    engine = Distributed(test_cluster_multiple_nodes_all_unavailable, system, one)"

trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
${CLICKHOUSE_CLIENT} \
    --opentelemetry-traceparent "00-$trace_id-0000000000000073-01" \
    --skip_unavailable_shards=1 \
    --use_hedged_requests=0 \
    --query "select count() from dist_05055_all_dead" 2>&1 \
    | grep -o -m1 'ALL_CONNECTION_TRIES_FAILED'
check_skipped_shard_span "$trace_id" "%skip_unavailable_shards%" \
    "every shard skipped on async path marks every span ERROR" 3 || exit 1

trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
${CLICKHOUSE_CLIENT} \
    --opentelemetry-traceparent "00-$trace_id-0000000000000073-01" \
    --skip_unavailable_shards=1 \
    --async_socket_for_remote=0 \
    --async_query_sending_for_remote=0 \
    --use_hedged_requests=0 \
    --query "select count() from dist_05055_all_dead" 2>&1 \
    | grep -o -m1 'ALL_CONNECTION_TRIES_FAILED'
check_skipped_shard_span "$trace_id" "%skip_unavailable_shards%" \
    "every shard skipped on sync path marks every span ERROR" 3 || exit 1
