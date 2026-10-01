#!/usr/bin/env bash
# Tags: distributed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Trace context propagation for a distributed SELECT must not depend on
# async_query_sending_for_remote. The query to the remote node is sent from inside the
# RemoteQueryExecutorReadContext fiber, which starts with an empty fiber-local tracing
# context; it used to lose the trace there: no CLIENT span was created and
# client_trace_context was not overridden, so the remote SERVER spans did not parent
# under the initiator. Now AsyncTaskExecutor seeds the fiber with the tracing context
# of the thread that created it and covers each task execution with one span.

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

function trace_counts_query
{
    local _trace_id="$1"
    echo "
        with UUIDNumToString(toFixedString(unhex('$_trace_id'), 16)) as t
        select
            -- CLIENT span for sending the remote query
            countIf(operation_name = 'Connection::sendQuery()' and kind = 'CLIENT'),
            -- span covering the fiber task execution
            countIf(operation_name = 'RemoteQueryExecutorReadContext'),
            -- remote SERVER handler parented under the CLIENT span
            (select count()
                from system.opentelemetry_span_log server_span,
                     system.opentelemetry_span_log client_span
                where server_span.finish_date >= yesterday() and server_span.trace_id = t
                  and client_span.finish_date >= yesterday() and client_span.trace_id = t
                  and server_span.operation_name = 'TCPHandler' and server_span.kind = 'SERVER'
                  and client_span.operation_name = 'Connection::sendQuery()' and client_span.kind = 'CLIENT'
                  and server_span.parent_span_id = client_span.span_id)
        from system.opentelemetry_span_log
        where finish_date >= yesterday() and trace_id = t
    "
}

for async_send in 0 1; do
    echo "=== async_query_sending_for_remote=$async_send ==="

    trace_id=$(${CLICKHOUSE_CLIENT} -q "select lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")

    ${CLICKHOUSE_CLIENT} \
        --opentelemetry-traceparent "00-$trace_id-0000000000000073-01" \
        --async_query_sending_for_remote="$async_send" \
        --async_socket_for_remote=1 \
        --query "select * from remote('127.0.0.2', system, one) format Null"

    poll_spans "$(trace_counts_query "$trace_id")" "1 1 1" || exit 1

    ${CLICKHOUSE_CLIENT} -q "
        with UUIDNumToString(toFixedString(unhex('$trace_id'), 16)) as t
        select
            if(countIf(operation_name = 'Connection::sendQuery()' and kind = 'CLIENT') >= 1,
               'sendQuery CLIENT span: OK', 'sendQuery CLIENT span: FAIL'),
            if(countIf(operation_name = 'RemoteQueryExecutorReadContext' and parent_span_id != 0) >= 1,
               'task span: OK', 'task span: FAIL')
        from system.opentelemetry_span_log
        where finish_date >= yesterday() and trace_id = t
        format TSV
    "

    # The remote 'query' span must be reachable from the initiator through the CLIENT span:
    # query <- TCPHandler (SERVER) <- Connection::sendQuery() (CLIENT).
    ${CLICKHOUSE_CLIENT} -q "
        with UUIDNumToString(toFixedString(unhex('$trace_id'), 16)) as t
        select if(count() >= 1, 'remote query parents under CLIENT span: OK',
                                'remote query parents under CLIENT span: FAIL')
        from system.opentelemetry_span_log query_span,
             system.opentelemetry_span_log server_span,
             system.opentelemetry_span_log client_span
        where query_span.finish_date >= yesterday() and query_span.trace_id = t
          and server_span.finish_date >= yesterday() and server_span.trace_id = t
          and client_span.finish_date >= yesterday() and client_span.trace_id = t
          and query_span.operation_name = 'query'
          and query_span.parent_span_id = server_span.span_id
          and server_span.operation_name = 'TCPHandler' and server_span.kind = 'SERVER'
          and server_span.parent_span_id = client_span.span_id
          and client_span.operation_name = 'Connection::sendQuery()' and client_span.kind = 'CLIENT'
        format TSV
    "

    # Sampled tracing (no inbound traceparent): the trace started on the initiator must
    # reach the remote side as the same trace.
    sampled_query_id="$CLICKHOUSE_TEST_UNIQUE_NAME-sampled-$async_send"
    ${CLICKHOUSE_CLIENT} \
        --opentelemetry_start_trace_probability=1 \
        --async_query_sending_for_remote="$async_send" \
        --async_socket_for_remote=1 \
        --query_id "$sampled_query_id" \
        --query "select * from remote('127.0.0.2', system, one) format Null"

    sampled_counts_query="
        with (select any(trace_id) from system.opentelemetry_span_log
              where finish_date >= yesterday() and operation_name = 'query'
                and attribute['clickhouse.query_id'] = '$sampled_query_id') as t
        select
            countIf(operation_name = 'query'),
            countIf(operation_name = 'Connection::sendQuery()' and kind = 'CLIENT')
        from system.opentelemetry_span_log
        where finish_date >= yesterday() and trace_id = t
    "
    poll_spans "$sampled_counts_query" "2 1" || exit 1

    ${CLICKHOUSE_CLIENT} -q "
        with (select any(trace_id) from system.opentelemetry_span_log
              where finish_date >= yesterday() and operation_name = 'query'
                and attribute['clickhouse.query_id'] = '$sampled_query_id') as t
        select
            if(countIf(operation_name = 'query') >= 2, 'sampled trace joins remote query: OK',
                                                       'sampled trace joins remote query: FAIL'),
            if(countIf(operation_name = 'Connection::sendQuery()' and kind = 'CLIENT') >= 1,
               'sampled sendQuery CLIENT span: OK', 'sampled sendQuery CLIENT span: FAIL')
        from system.opentelemetry_span_log
        where finish_date >= yesterday() and trace_id = t
        format TSV
    "
done

# ConnectionEstablisherAsync and PacketReceiver (hedged requests) are Linux-only and are
# covered separately by 04926_opentelemetry_hedged_remote_query_spans.

# Cancellation: destroying the fiber while it is suspended unwinds its stack outside of
# resume(); the tracing holder destructor must still resolve the fiber-local context and
# emit the task span, and must not corrupt the tracing context of the cancelling thread.
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
# system.processes (127.0.0.2 loops back to this same server) only after
# Connection::sendQuery succeeded, and with async_query_sending_for_remote=1 sendQuery
# runs inside the RemoteQueryExecutorReadContext fiber, so its presence proves the fiber
# is created and suspended. Waiting only for the initiator query would race with query
# startup: a kill landing before the first resume finds no fiber to unwind and no task
# span is ever emitted.
for _retry in {1..100}; do
    started=$(${CLICKHOUSE_CLIENT} -q "select count() from system.processes where initial_query_id = '$kill_query_id' and query_id != initial_query_id")
    [[ "$started" -ge 1 ]] && break
    sleep 0.1
done
${CLICKHOUSE_CLIENT} -q "kill query where query_id = '$kill_query_id' sync format Null"
wait

poll_spans "
    with UUIDNumToString(toFixedString(unhex('$trace_id'), 16)) as t
    select countIf(operation_name = 'RemoteQueryExecutorReadContext')
    from system.opentelemetry_span_log
    where finish_date >= yesterday() and trace_id = t" "1" \
|| exit 1
echo "task span emitted on cancellation: OK"

${CLICKHOUSE_CLIENT} -q "select 'server is alive: OK'"
