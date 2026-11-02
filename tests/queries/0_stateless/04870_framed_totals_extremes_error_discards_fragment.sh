#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: enables the `framing_throw_before_totals_boundary` /
# `framing_throw_before_extremes_boundary` fail points, which affect the whole server. Each fires on
# the next framed totals/extremes boundary anywhere on the server, so a concurrent framing query
# from another test could consume the injected fault - making this test miss its own injection and
# the other test throw spuriously.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The concatenation of the `data` payloads of a framed response is documented to be exactly the
# format's main output. When the serialization of a `totals` / `extremes` portion throws after
# buffering some bytes but before its packet boundary is taken, the exception path of
# `IFramingFormat::finalize` must DISCARD the buffered fragment: flushing it would either mislabel
# it as a `data` packet (handing a client that concatenates the `data` payloads bytes that never
# belonged to the main result set) or emit a partial row under its own packet kind. The stream is
# terminal - the `exception` packet follows - so nothing is lost by discarding.
#
# `http_wait_end_of_query` and `http_response_buffer_size` are pinned because the test harness
# randomizes them and a nonzero value routes the output through a cascade buffer (a different
# path); `interactive_delay` is pinned to one hour so no throttled `progress` packet lands in the
# stream on a slow run; `max_threads=1` and `output_format_parallel_formatting=0` keep the main
# result a single deterministic `data` packet.

URL_STREAMING="${CLICKHOUSE_URL}&framing_output_format=JSONEachPacketString&http_wait_end_of_query=0&http_response_buffer_size=0&max_threads=1&output_format_parallel_formatting=0&interactive_delay=3600000000"

TOTALS_QUERY="SELECT intDiv(number, 2) AS k, count() AS c FROM numbers(4) GROUP BY k WITH TOTALS ORDER BY k FORMAT TSV"
EXTREMES_QUERY="SELECT number FROM numbers(3) ORDER BY number FORMAT TSV"

echo '--- a throw while a totals portion is buffered discards the fragment'
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT framing_throw_before_totals_boundary"
RESPONSE=$(${CLICKHOUSE_CURL} -s "${URL_STREAMING}" -d "${TOTALS_QUERY}")
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT framing_throw_before_totals_boundary"
# The main result was streamed before the fault: exactly one data packet, carrying only the rows.
[ "$(echo "$RESPONSE" | grep -c '"packet":"data"')" -eq 1 ] && echo 'exactly one data packet: OK'
echo "$RESPONSE" | grep -q -F '"data":"0\t2\n1\t2\n"' && echo 'the data packet carries only the main rows: OK'
# The buffered totals fragment (the row `0	4`) was discarded: it appears neither as a stray
# `data` packet nor under a `totals` packet.
echo "$RESPONSE" | grep -q -F '0\t4' && echo "MISMATCH: the totals fragment leaked: $RESPONSE" || echo 'no totals fragment in the stream: OK'
echo "$RESPONSE" | grep -q '"packet":"totals"' && echo "MISMATCH: a totals packet was emitted: $RESPONSE" || echo 'no totals packet: OK'
# The stream is terminal: the last packet is the injected exception.
echo "$RESPONSE" | tail -n 1 | grep -q '"packet":"exception"' && echo 'the exception packet is terminal: OK'
echo "$RESPONSE" | tail -n 1 | grep -q -F 'FAULT_INJECTED' && echo 'the exception carries the injected fault: OK'

echo '--- a throw while an extremes portion is buffered discards the fragment'
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT framing_throw_before_extremes_boundary"
RESPONSE=$(${CLICKHOUSE_CURL} -s "${URL_STREAMING}&extremes=1" -d "${EXTREMES_QUERY}")
${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT framing_throw_before_extremes_boundary"
[ "$(echo "$RESPONSE" | grep -c '"packet":"data"')" -eq 1 ] && echo 'exactly one data packet: OK'
echo "$RESPONSE" | grep -q -F '"data":"0\n1\n2\n"' && echo 'the data packet carries only the main rows: OK'
echo "$RESPONSE" | grep -q '"packet":"extremes"' && echo "MISMATCH: an extremes packet was emitted: $RESPONSE" || echo 'no extremes packet: OK'
echo "$RESPONSE" | tail -n 1 | grep -q '"packet":"exception"' && echo 'the exception packet is terminal: OK'
echo "$RESPONSE" | tail -n 1 | grep -q -F 'FAULT_INJECTED' && echo 'the exception carries the injected fault: OK'

echo '--- without the fault the totals and extremes packets are emitted normally'
RESPONSE=$(${CLICKHOUSE_CURL} -s "${URL_STREAMING}" -d "${TOTALS_QUERY}")
echo "$RESPONSE" | grep -q -F '"packet":"totals","data":"\n0\t4\n"' && echo 'the totals packet is intact: OK'
RESPONSE=$(${CLICKHOUSE_CURL} -s "${URL_STREAMING}&extremes=1" -d "${EXTREMES_QUERY}")
echo "$RESPONSE" | grep -q -F '"packet":"extremes","data":"\n0\n2\n"' && echo 'the extremes packet is intact: OK'
