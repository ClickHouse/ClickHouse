#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: enables the `framing_exception_packet_throw` fail point, which affects the whole
# server. It fires on the next framed exception delivery anywhere on the server, so a concurrent
# framing query from another test could consume the injected fault - making this test miss its own
# truncation and the other test throw spuriously.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The HTTP handler's fail-close guard for framed responses must not depend on the
# `set_result_details` callback: `executeQuery` deliberately swallows a failure of that callback on
# the exception path (emulated here by `execute_query_calling_empty_set_result_func_on_exception`),
# yet the response is still written through a framing format afterwards. So when a second failure
# hits while the framed exception is being delivered (`framing_exception_packet_throw`), after the
# auxiliary packets are already on the wire, the response must fail closed: nothing may be appended
# to the started packet stream - in particular not the generic `__exception__` block that
# `cancelWithException` writes.
#
# `send_logs_level` makes the framed exception response start with `log` packets, so the stream has
# really begun before the injected failure; the response must actually stream, so
# `http_wait_end_of_query` and `http_response_buffer_size` are pinned (the harness randomizes both).

URL="${CLICKHOUSE_URL}&framing_output_format=JSONEachPacketString&send_logs_level=trace&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"

echo '--- a framed exception response fails closed even when set_result_details never ran'
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT execute_query_calling_empty_set_result_func_on_exception"
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT framing_exception_packet_throw"
RESPONSE=$(${CLICKHOUSE_CURL} -s "${URL}" -d "SELECT 1 FROM table_04843_does_not_exist FORMAT JSONEachRow")
CURL_EXIT=$?
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT framing_exception_packet_throw"
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT execute_query_calling_empty_set_result_func_on_exception"

[[ "$RESPONSE" == *'"packet":"log"'* ]] && echo 'the packet stream had started: OK' \
    || echo "MISMATCH: no log packet received: $RESPONSE"
[[ "$RESPONSE" != *'__exception__'* ]] && echo 'the generic __exception__ block was not appended: OK' \
    || echo "MISMATCH: the generic __exception__ block was appended: $RESPONSE"
[[ "$RESPONSE" != *'FAULT_INJECTED'* ]] && echo 'the secondary failure was not reported in the body: OK' \
    || echo "MISMATCH: the secondary failure was appended: $RESPONSE"
[[ $CURL_EXIT -ne 0 ]] && echo 'the client observes an aborted connection: OK' \
    || echo "MISMATCH: curl succeeded on a truncated response (response: $RESPONSE)"
