#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: enables the `framing_exception_packet_throw` fail point, which affects the whole
# server. It fires on the next framed exception delivery anywhere on the server, so a concurrent
# framing query from another test could consume the injected fault - making this test miss its own
# truncation and the other test throw spuriously.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A framed response fails closed as soon as framed packet bytes have been produced into the
# response stream, even when they are still sitting in the server-side buffers and the HTTP
# transmission has not started. Here the query fails before emitting any output, so nothing has
# been flushed; the framed exception delivery first writes the auxiliary packets (profile events)
# into the response buffer and then hits the injected fault before the terminal `exception`
# packet. `cancelWithException` cannot deliver a clean error in that state - it keeps non-empty
# buffers and appends the plain error message after them - so falling through to it would deliver
# the buffered packets followed by a plain error body: a mixed response that is not a stream of
# packets. The response must be aborted without delivering anything instead.
#
# Both `http_wait_end_of_query` and `http_response_buffer_size` are pinned because the test
# harness randomizes them and each nonzero value routes the output through a cascade buffer,
# which is a different path (tested below). `interactive_delay` is pinned to one hour because
# framed `progress` packets are throttled by it: with the default 100 ms, a slow run of the
# failing query emits a `progress` packet before the fault fires, and it is flushed straight to
# the client (the response buffer is zero), so the aborted response would not be empty.

URL_STREAMING="${CLICKHOUSE_URL}&framing_output_format=JSONEachPacketString&http_wait_end_of_query=0&http_response_buffer_size=0&max_threads=1&output_format_parallel_formatting=0&send_profile_events=1&interactive_delay=3600000000"

echo '--- a failure while delivering the framed exception aborts a buffered, not yet started response'
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT framing_exception_packet_throw"
RESPONSE=$(${CLICKHOUSE_CURL} -s "${URL_STREAMING}" -d "SELECT throwIf(1) FORMAT JSONEachRow")
CURL_EXIT=$?
[[ $CURL_EXIT -ne 0 ]] && echo 'the client observes an aborted connection: OK' \
    || echo "MISMATCH: curl succeeded on an aborted response (response: $RESPONSE)"
if [[ -z "$RESPONSE" ]]; then
    echo 'nothing was delivered: OK'
elif [[ "$RESPONSE" == *'FAULT_INJECTED'* ]] || [[ "$RESPONSE" == *'__exception__'* ]]; then
    echo "MISMATCH: a plain error body was delivered on the framed response: $RESPONSE"
else
    echo "MISMATCH: something was delivered: $RESPONSE"
fi
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT framing_exception_packet_throw"

# With `http_response_buffer_size` large enough to hold the whole output, the packets buffered in
# the memory cascade never reach the response stream, and on a failure they are discarded cleanly
# (nothing of the packet stream can have been delivered), so the client gets a proper plain HTTP
# error instead of an aborted connection. This pins that the fail-close above does not fire
# spuriously for output that is still fully discardable.

URL_BUFFERED="${CLICKHOUSE_URL}&framing_output_format=JSONEachPacketString&http_wait_end_of_query=0&http_response_buffer_size=1048576&max_threads=1&max_block_size=1&output_format_parallel_formatting=0"

echo '--- a fully buffered response is discarded and replaced by a plain HTTP error'
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT framing_exception_packet_throw"
RESPONSE=$(${CLICKHOUSE_CURL} -s "${URL_BUFFERED}" \
    -d "SELECT number AS x, throwIf(number = 2) AS t FROM numbers(3) FORMAT JSONEachRow")
CURL_EXIT=$?
[[ $CURL_EXIT -eq 0 ]] && echo 'the client receives a complete HTTP response: OK' \
    || echo "MISMATCH: curl failed on the buffered response (exit $CURL_EXIT, response: $RESPONSE)"
if [[ "$RESPONSE" == *'"packet":'* ]]; then
    echo "MISMATCH: buffered packets were delivered: $RESPONSE"
elif [[ "$RESPONSE" == *'FAULT_INJECTED'* ]]; then
    echo 'the discarded stream was replaced by a plain error: OK'
else
    echo "MISMATCH: unexpected response: $RESPONSE"
fi
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT framing_exception_packet_throw"
