#!/usr/bin/env bash
# Tags: distributed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A failed remote shard read must be recorded on the per-shard fragment span
# (`RemoteQueryExecutor::execute`) as status ERROR with the exception message. On the default
# asynchronous path the span is owned by the read context fiber and used to be logged with
# status UNSET on failure: nothing stamped it when the fiber task threw locally (the exception
# is caught in the `AsyncTaskExecutor` routine and rethrown on the consumer thread only after
# the span was logged), and an exception packet from the shard is processed on the consumer
# thread, which stamped only the synchronous-path span.

function check_error_span
{
    local _trace_id="$1"
    local _label="$2"
    local _query="
        with UUIDNumToString(toFixedString(unhex('$_trace_id'), 16)) as t
        select countIf(status_code = 'ERROR' and status_message != '')
        from system.opentelemetry_span_log
        where finish_date >= yesterday() and trace_id = t
          and operation_name = 'RemoteQueryExecutor::execute'"
    # Spans are flushed to the log by background threads, poll until the span appears.
    for _retry in {1..20}; do
        ${CLICKHOUSE_CLIENT} -q "system flush logs opentelemetry_span_log"
        if [[ "$(${CLICKHOUSE_CLIENT} -q "$_query")" -ge 1 ]]; then
            echo "$_label: OK"
            return 0
        fi
        sleep 1
    done
    echo "$_label: FAIL"
    return 1
}

# Local failure inside the read context fiber: the network byte limit trips in the throttler
# while receiving the shard's data, so the exception is thrown from inside the fiber.
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
${CLICKHOUSE_CLIENT} \
    --opentelemetry-traceparent "00-$trace_id-0000000000000073-01" \
    --max_network_bytes=1 \
    --use_hedged_requests=0 \
    --query "select * from remote('127.0.0.2', numbers(1000000)) format Null" 2>/dev/null
check_error_span "$trace_id" "local failure on async path marks span ERROR" || exit 1

# Remote failure: the shard terminates the query with an exception packet, which is processed
# on the consumer thread and must reach the fiber-owned span through the buffered status.
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
${CLICKHOUSE_CLIENT} \
    --opentelemetry-traceparent "00-$trace_id-0000000000000073-01" \
    --use_hedged_requests=0 \
    --query "select * from remote('127.0.0.2', view(select throwIf(1) from system.one)) format Null" 2>/dev/null
check_error_span "$trace_id" "remote error on async path marks span ERROR" || exit 1

# The same remote failure on the fully synchronous path, where the fragment span is the
# detached synchronous-path span.
trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
${CLICKHOUSE_CLIENT} \
    --opentelemetry-traceparent "00-$trace_id-0000000000000073-01" \
    --async_socket_for_remote=0 \
    --async_query_sending_for_remote=0 \
    --use_hedged_requests=0 \
    --query "select * from remote('127.0.0.2', view(select throwIf(1) from system.one)) format Null" 2>/dev/null
check_error_span "$trace_id" "remote error on sync path marks span ERROR" || exit 1
