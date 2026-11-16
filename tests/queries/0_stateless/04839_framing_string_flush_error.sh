#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: enables the `write_buffer_valid_utf8_finalize_throw` fail point, which affects the
# whole server. It fires on the next flush of a UTF-8 validating buffer anywhere on the server, so a
# concurrent query could consume the injected fault - making this test miss its own failure and the
# other query throw spuriously.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The string fields of the auxiliary framed packets (`log`, `profile_events`, `exception`) are written
# through a UTF-8 validating buffer, whose tail is flushed only when the buffer is finalized. Its
# destructor suppresses a failure of that flush, so the framing format finalizes the buffer itself:
# otherwise a failure to write the last bytes of such a string would be lost, leaving a truncated -
# hence invalid - JSON packet on the wire while the stream happily continues to its success
# terminator. Both cases below inject that failure and check that the response fails closed instead.
#
# `http_wait_end_of_query` and `http_response_buffer_size` are pinned because the test harness
# randomizes them and each nonzero value routes the output through a cascade buffer, from which the
# packets are still fully discardable (that path is covered by
# `04817_framing_error_with_buffered_response`). `interactive_delay` is pinned to one hour so that
# throttled `progress` packets do not interleave with the assertions.

URL="${CLICKHOUSE_URL}&framing_output_format=JSONEachPacketString&http_wait_end_of_query=0&http_response_buffer_size=0&max_threads=1&output_format_parallel_formatting=0&interactive_delay=3600000000"

echo '--- a failed string flush in a profile events packet fails the stream closed'
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT write_buffer_valid_utf8_finalize_throw"
RESPONSE=$(${CLICKHOUSE_CURL} -s "${URL}&send_profile_events=1" -d "SELECT 1 AS x FORMAT JSONEachRow")
# The query itself succeeds and its `data` packet has already been delivered when the flush of the
# `profile_events` packet fails, so the bytes of a half-written packet are on the wire and the stream
# fails closed: nothing more is written to it. In particular it is not terminated - neither by the
# final `progress` packet (whose `result_rows` is the success terminator of a framed stream, and
# which the previous behavior happily wrote after the truncated packet) nor by an `exception` packet,
# and no plain error body is appended to the packet stream either.
if [[ "$RESPONSE" != *'"packet":"data"'* ]]; then
    echo "MISMATCH: the data packet was not delivered: $RESPONSE"
elif [[ "$RESPONSE" == *'result_rows'* ]]; then
    echo "MISMATCH: the stream was terminated as successful: $RESPONSE"
elif [[ "$RESPONSE" == *'"packet":"exception"'* ]]; then
    echo "MISMATCH: the stream was terminated after a half-written packet: $RESPONSE"
elif [[ "$RESPONSE" == *'FAULT_INJECTED'* ]] || [[ "$RESPONSE" == *'__exception__'* ]]; then
    echo "MISMATCH: a plain error body was appended to the packet stream: $RESPONSE"
else
    echo 'the stream was not terminated: OK'
fi
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT write_buffer_valid_utf8_finalize_throw"

echo '--- a failed string flush in the exception packet aborts the response'
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT write_buffer_valid_utf8_finalize_throw"
RESPONSE=$(${CLICKHOUSE_CURL} -s "${URL}&send_profile_events=0" -d "SELECT throwIf(1) FORMAT JSONEachRow")
CURL_EXIT=$?
[[ $CURL_EXIT -ne 0 ]] && echo 'the client observes an aborted connection: OK' \
    || echo "MISMATCH: curl succeeded on a truncated exception packet (response: $RESPONSE)"
# A truncated `exception` packet must not be presented as a complete one, and the generic HTTP error
# path must not append a plain error body to it either.
if [[ "$RESPONSE" == *'FAULT_INJECTED'* ]] || [[ "$RESPONSE" == *'__exception__'* ]]; then
    echo "MISMATCH: a plain error body was delivered on the framed response: $RESPONSE"
else
    echo 'no complete exception packet was delivered: OK'
fi
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT write_buffer_valid_utf8_finalize_throw"
