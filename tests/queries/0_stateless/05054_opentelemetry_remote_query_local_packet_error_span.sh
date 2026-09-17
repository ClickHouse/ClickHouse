#!/usr/bin/env bash
# Tags: distributed, no-parallel
# no-parallel: the failpoint is server-wide and would fail any concurrent distributed query.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A non-`Server::Exception` failure raised by `processPacket` on the consumer thread (not inside
# the read context fiber) must be recorded on the per-shard fragment span
# (`RemoteQueryExecutor::execute`) as status ERROR. This path used to bypass the span backstops:
# the exception escaped `readAsync` and the destructor logged the span as OK.

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

${CLICKHOUSE_CLIENT} -q "system enable failpoint remote_query_executor_local_packet_processing_error"

# The failpoint fires while processing a `Data` packet on the consumer thread, on both paths.

# Default asynchronous path: the span is owned by the read context fiber.
trace_id_async=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
${CLICKHOUSE_CLIENT} \
    --opentelemetry-traceparent "00-$trace_id_async-0000000000000073-01" \
    --use_hedged_requests=0 \
    --query "select * from remote('127.0.0.2', numbers(1000000)) format Null" 2>/dev/null

# Fully synchronous path: the span is the detached synchronous-path span.
trace_id_sync=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")
${CLICKHOUSE_CLIENT} \
    --opentelemetry-traceparent "00-$trace_id_sync-0000000000000073-01" \
    --async_socket_for_remote=0 \
    --async_query_sending_for_remote=0 \
    --use_hedged_requests=0 \
    --query "select * from remote('127.0.0.2', numbers(1000000)) format Null" 2>/dev/null

${CLICKHOUSE_CLIENT} -q "system disable failpoint remote_query_executor_local_packet_processing_error"

check_error_span "$trace_id_async" "local packet-processing error on async path marks span ERROR" || exit 1
check_error_span "$trace_id_sync" "local packet-processing error on sync path marks span ERROR" || exit 1
