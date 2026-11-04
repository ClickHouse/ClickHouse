#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: enables the `framing_exception_packet_throw` and `http_output_finalize_throw` fail
# points, which affect the whole server. They fire on the next framed exception delivery / response
# close anywhere on the server, so a concurrent query from another test could consume the injected
# fault - making this test miss its own failure and the other test throw spuriously.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A framed response must fail closed even when the `set_result_details` callback never ran.
# `executeQuery` deliberately swallows a failure of that callback on the exception path (emulated
# here by `execute_query_calling_empty_set_result_func_on_exception`; see `04629`), yet the response
# is then still written through a framing format - so the HTTP handler latches its fail-close guard
# (`used_output.framed`) from the output format's framing directly, not only from that callback.
# When a SECOND failure hits while the framed exception is delivered, or while the response is
# closed, nothing may be appended to the packet stream - in particular not the generic
# `__exception__` block that `cancelWithException` writes. The client sees a truncated response and
# an aborted connection instead.
#
# The response must actually stream, so `http_wait_end_of_query` and `http_response_buffer_size` are
# pinned (the harness randomizes both).

URL="${CLICKHOUSE_URL}&framing_output_format=JSONEachPacketString&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"
QUERY="SELECT 1 FROM table_04843_does_not_exist FORMAT JSONEachRow"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT execute_query_calling_empty_set_result_func_on_exception"

echo '--- a failure while delivering the framed exception appends nothing'
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT framing_exception_packet_throw"
RESPONSE=$(${CLICKHOUSE_CURL} -s "${URL}" -d "$QUERY")
CURL_EXIT=$?
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT framing_exception_packet_throw"
[[ $CURL_EXIT -ne 0 ]] && echo 'the client observes an aborted connection: OK' \
    || echo "MISMATCH: curl succeeded on a truncated response (response: $RESPONSE)"
[[ "$RESPONSE" != *'__exception__'* ]] && echo 'no generic __exception__ block: OK' \
    || echo "MISMATCH: the generic __exception__ block was appended: $RESPONSE"
[[ "$RESPONSE" != *'FAULT_INJECTED'* ]] && echo 'the secondary failure is not in the body: OK' \
    || echo "MISMATCH: the secondary failure was appended: $RESPONSE"

echo '--- a failure while closing the response appends nothing after the exception packet'
# Here the framed exception packet itself is written and flushed before the injected failure, so the
# stream the client receives is a complete packet stream - and still nothing follows it.
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT http_output_finalize_throw"
RESPONSE=$(${CLICKHOUSE_CURL} -s "${URL}" -d "$QUERY")
CURL_EXIT=$?
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT http_output_finalize_throw"
[[ $CURL_EXIT -ne 0 ]] && echo 'the client observes an aborted connection: OK' \
    || echo "MISMATCH: curl succeeded on a truncated response (response: $RESPONSE)"
[[ "$RESPONSE" == *'"packet":"exception"'* ]] && echo 'the framed exception packet was received: OK' \
    || echo "MISMATCH: no exception packet received: $RESPONSE"
[[ "$RESPONSE" != *'__exception__'* ]] && echo 'no generic __exception__ block: OK' \
    || echo "MISMATCH: the generic __exception__ block was appended: $RESPONSE"
[[ "$(echo "$RESPONSE" | grep -c '"packet":"exception"')" -eq 1 ]] && echo 'exactly one exception packet: OK' \
    || echo "MISMATCH: the failed close appended a second framed exception: $RESPONSE"

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT execute_query_calling_empty_set_result_func_on_exception"
