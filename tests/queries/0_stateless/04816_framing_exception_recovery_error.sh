#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: enables the `framing_exception_packet_throw` fail point, which affects the whole
# server. It fires on the next framed exception delivery anywhere on the server, so a concurrent
# framing query from another test could consume the injected fault - making this test miss its own
# truncation and the other test throw spuriously.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When the framed exception recovery itself fails - here a fault injected while the terminal
# `exception` packet is being delivered, after `data` packets have already been streamed - the
# response fails closed: nothing may be appended to the already-started packet stream, neither a
# second framed response nor the generic `__exception__` block that `cancelWithException` writes
# into an already-sent response. The client observes the complete packets written before the
# failure, then a truncated response and an aborted connection.
#
# The response must actually stream, so `http_wait_end_of_query` and `http_response_buffer_size`
# are pinned (the test harness randomizes both), and the single-threaded row-by-row processing
# guarantees that `data` packets are on the wire before the query fails on the last row.

URL="${CLICKHOUSE_URL}&framing_output_format=JSONEachPacketString&http_wait_end_of_query=0&http_response_buffer_size=0&max_threads=1&max_block_size=1&output_format_parallel_formatting=0"

echo '--- a failure while delivering the framed exception truncates the response'
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT framing_exception_packet_throw"
RESPONSE=$(${CLICKHOUSE_CURL} -s "${URL}" \
    -d "SELECT number AS x, throwIf(number = 2) AS t FROM numbers(3) FORMAT JSONEachRow")
CURL_EXIT=$?
[[ $CURL_EXIT -ne 0 ]] && echo 'the client observes an aborted connection: OK' \
    || echo "MISMATCH: curl succeeded on a truncated response (response: $RESPONSE)"
[[ "$RESPONSE" == *'"packet":"data"'* ]] && echo 'the partial success stream was received: OK' \
    || echo "MISMATCH: no data packet received: $RESPONSE"
if [[ "$RESPONSE" == *'__exception__'* ]]; then
    echo 'MISMATCH: the generic __exception__ block was appended'
elif [[ "$RESPONSE" == *'"packet":"exception"'* ]] || [[ "$RESPONSE" == *'FAULT_INJECTED'* ]]; then
    echo 'MISMATCH: an exception packet was appended by the failed recovery'
else
    echo 'nothing appended after the failure: OK'
fi
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT framing_exception_packet_throw"
