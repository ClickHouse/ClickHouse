#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Real-time previews of the query result (the `query_result_previews` setting) over the HTTP
# protocol with framing formats: previews are emitted as `preview` packets. The number of previews
# depends on timing and scheduling, so the tests below assert only invariants:
#   - at least one `preview` packet is emitted (the thresholds are zeroed to fire on every block);
#   - every `preview` payload is a complete document of the payload format;
#   - no `preview` packet follows the first `data` packet;
#   - the `data` payloads are byte-identical to a run without previews;
#   - the final counters (`result_rows`) do not include preview rows.

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"
PREVIEWS="&query_result_previews=1&query_result_previews_min_interval_ms=0"
# Pin the settings that determine the number of source blocks, so that enough blocks are consumed
# by the aggregation to emit previews regardless of the settings randomization in CI.
BLOCKS="&max_block_size=65536&max_threads=4&group_by_two_level_threshold=100000000&group_by_two_level_threshold_bytes=1000000000"
BLOCKS="${BLOCKS}&max_bytes_before_external_group_by=0&max_bytes_ratio_before_external_group_by=0&enable_adaptive_aggregator=0"
QUERY="SELECT intDiv(number, 250000) AS k, count() AS c FROM numbers(1000000) GROUP BY k ORDER BY k FORMAT JSONCompactEachRow"

run_framed()
{
    ${CLICKHOUSE_CURL} -sS "${URL}${BLOCKS}$1&framing_output_format=JSONEachPacketString" -d "${QUERY}"
}

echo '--- previews are emitted and every preview payload is a complete document'
RESPONSE=$(run_framed "${PREVIEWS}")
PREVIEWS_COUNT=$(echo "${RESPONSE}" | grep -c '"packet":"preview"')
if [ "${PREVIEWS_COUNT}" -ge 1 ]; then echo "has previews"; else echo "no previews: ${RESPONSE}"; fi

# Every preview payload must parse as complete rows of the payload format: two columns, and the
# count column of every row must be a number not exceeding the per-key total.
echo "${RESPONSE}" | grep '"packet":"preview"' | python3 -c "
import json, sys
ok = True
for line in sys.stdin:
    packet = json.loads(line)
    for row_line in packet['data'].rstrip('\n').split('\n'):
        row = json.loads(row_line)
        if len(row) != 2 or not (0 < int(row[1]) <= 250000):
            ok = False
print('previews are well-formed' if ok else 'malformed previews')
"

echo '--- no preview packet follows the first data packet'
echo "${RESPONSE}" | awk '/"packet":"data"/{data=1} /"packet":"preview"/{if (data) {print "preview after data"; exit}} END {if (!data) print "no data packets"; else print "ok"}'

echo '--- the data payloads are identical with and without previews'
WITH_PREVIEWS=$(echo "${RESPONSE}" | grep '"packet":"data"')
WITHOUT_PREVIEWS=$(run_framed "" | grep '"packet":"data"')
if [ "${WITH_PREVIEWS}" == "${WITHOUT_PREVIEWS}" ]; then echo "identical"; else echo "differ: ${WITH_PREVIEWS} vs ${WITHOUT_PREVIEWS}"; fi

echo '--- the final counters do not include preview rows'
echo "${RESPONSE}" | grep '"result_rows"' | tail -n1 | grep -o '"result_rows":"4"' || echo "unexpected result_rows"

echo '--- previews of a sorting with a limit'
SORT_QUERY="SELECT number AS n FROM numbers(1000000) ORDER BY intHash64(n) LIMIT 3 FORMAT JSONCompactEachRow"
SORT_RESPONSE=$(${CLICKHOUSE_CURL} -sS "${URL}${BLOCKS}${PREVIEWS}&framing_output_format=JSONEachPacketString&optimize_read_in_order=0" -d "${SORT_QUERY}")
SORT_PREVIEWS=$(echo "${SORT_RESPONSE}" | grep -c '"packet":"preview"')
if [ "${SORT_PREVIEWS}" -ge 1 ]; then echo "has previews"; else echo "no previews: ${SORT_RESPONSE}"; fi
echo "${SORT_RESPONSE}" | grep '"packet":"preview"' | python3 -c "
import json, sys
ok = True
for line in sys.stdin:
    packet = json.loads(line)
    rows = packet['data'].rstrip('\n').split('\n')
    if not (1 <= len(rows) <= 3):
        ok = False
print('previews are cut to the limit' if ok else 'previews exceed the limit')
"

echo '--- without framing, previews are not emitted into the plain output'
PLAIN_WITH=$(${CLICKHOUSE_CURL} -sS "${URL}${BLOCKS}${PREVIEWS}" -d "${QUERY}")
PLAIN_WITHOUT=$(${CLICKHOUSE_CURL} -sS "${URL}${BLOCKS}" -d "${QUERY}")
if [ "${PLAIN_WITH}" == "${PLAIN_WITHOUT}" ]; then echo "identical"; else echo "differ: ${PLAIN_WITH} vs ${PLAIN_WITHOUT}"; fi
