#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: enables the `http_output_finalize_throw` fail point, which affects the whole
# server. It fires on the next HTTP response finalization anywhere on the server, so a concurrent
# HTTP query from another test could consume the injected fault - making this test miss its own
# truncation and the other test throw spuriously.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A failure while the framed response stream is being closed (`HTTPHandler::Output::finalize`:
# pushing the delayed results, finalizing the compression, closing the socket) must fail closed.
# By that point some (or all) of the framed success stream is already on the wire, so nothing may
# be appended to it: neither a fresh framed `exception` stream nor the generic `__exception__`
# block that `cancelWithException` writes into an already-sent response - either would follow a
# partial success response, breaking the "always a stream of packets" contract. The client must
# observe a truncated response and an aborted connection instead, like for a half-written packet.

check_response()
{
    local response="$1"
    if [[ "$response" == *'__exception__'* ]]; then
        echo 'MISMATCH: the generic __exception__ block was appended'
    elif [[ "$response" == *'"packet":"exception"'* ]] || [[ "$response" == *'FAULT_INJECTED'* ]]; then
        echo 'MISMATCH: an exception packet was appended to a stream that already started closing'
    else
        echo 'nothing appended after the failure: OK'
    fi
}

echo '--- streaming: a failure while closing the response truncates it'
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT http_output_finalize_throw"
# The streaming case asserts that the packets written before the failure did reach the client, so the
# response must actually stream: pin `http_wait_end_of_query` and `http_response_buffer_size` (the
# test harness randomizes both), or the data packet would sit in a server-side buffer that the failing
# finalization discards, and the client would receive nothing at all.
RESPONSE=$(${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}&framing_output_format=JSONEachPacketString&http_wait_end_of_query=0&http_response_buffer_size=0" \
    -d "SELECT number AS x FROM numbers(3) FORMAT JSONEachRow")
CURL_EXIT=$?
[[ $CURL_EXIT -ne 0 ]] && echo 'the client observes an aborted connection: OK' \
    || echo 'MISMATCH: curl succeeded on a truncated response'
[[ "$RESPONSE" == *'"packet":"data"'* ]] && echo 'the partial success stream was received: OK' \
    || echo "MISMATCH: no data packet received: $RESPONSE"
check_response "$RESPONSE"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT http_output_finalize_throw"

echo '--- buffered: a failure while closing the response truncates it'
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT http_output_finalize_throw"
# Use the `http_wait_end_of_query` setting rather than the `wait_end_of_query` URL parameter: the
# parameter is ignored when an `http_wait_end_of_query` setting is present in the URL, which the test
# harness randomizes.
RESPONSE=$(${CLICKHOUSE_CURL} -s "${CLICKHOUSE_URL}&framing_output_format=JSONEachPacketString&http_wait_end_of_query=1" \
    -d "SELECT number AS x FROM numbers(3) FORMAT JSONEachRow")
CURL_EXIT=$?
[[ $CURL_EXIT -ne 0 ]] && echo 'the client observes an aborted connection: OK' \
    || echo "MISMATCH: curl succeeded on a truncated response (response: $RESPONSE)"
check_response "$RESPONSE"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT http_output_finalize_throw"
