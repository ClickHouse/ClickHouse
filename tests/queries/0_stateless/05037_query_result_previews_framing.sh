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

echo '--- DISTINCT deduplicates each preview standalone'
DISTINCT_QUERY="SELECT DISTINCT c FROM (SELECT intDiv(number, 250000) AS k, count() AS c FROM numbers(1000000) GROUP BY k) ORDER BY c FORMAT JSONCompactEachRow"
DISTINCT_RESPONSE=$(${CLICKHOUSE_CURL} -sS "${URL}${BLOCKS}${PREVIEWS}&framing_output_format=JSONEachPacketString" -d "${DISTINCT_QUERY}")
DISTINCT_PREVIEWS=$(echo "${DISTINCT_RESPONSE}" | grep -c '"packet":"preview"')
if [ "${DISTINCT_PREVIEWS}" -ge 1 ]; then echo "has previews"; else echo "no previews: ${DISTINCT_RESPONSE}"; fi
echo "${DISTINCT_RESPONSE}" | grep '"packet":"preview"' | python3 -c "
import json, sys
ok = True
for line in sys.stdin:
    rows = json.loads(line)['data'].rstrip('\n').split('\n')
    if len(rows) != len(set(rows)):
        ok = False
print('previews are deduplicated' if ok else 'duplicate rows in a preview')
"
echo "${DISTINCT_RESPONSE}" | grep '"packet":"data"'

echo '--- window functions are computed over each preview standalone'
WINDOW_QUERY="SELECT k, round(c / max(c) OVER (), 6) AS share FROM (SELECT intDiv(number, 250000) AS k, count() AS c FROM numbers(1000000) GROUP BY k) ORDER BY k FORMAT JSONCompactEachRow"
WINDOW_RESPONSE=$(${CLICKHOUSE_CURL} -sS "${URL}${BLOCKS}${PREVIEWS}&framing_output_format=JSONEachPacketString" -d "${WINDOW_QUERY}")
WINDOW_PREVIEWS=$(echo "${WINDOW_RESPONSE}" | grep -c '"packet":"preview"')
if [ "${WINDOW_PREVIEWS}" -ge 1 ]; then echo "has previews"; else echo "no previews: ${WINDOW_RESPONSE}"; fi
echo "${WINDOW_RESPONSE}" | grep '"packet":"preview"' | python3 -c "
import json, sys
ok = True
for line in sys.stdin:
    rows = [json.loads(r) for r in json.loads(line)['data'].rstrip('\n').split('\n')]
    shares = [float(r[1]) for r in rows]
    if abs(max(shares) - 1.0) > 1e-9 or any(not (0 < s <= 1) for s in shares):
        ok = False
print('previews carry their own window maximum' if ok else 'wrong window values in a preview')
"
echo "${WINDOW_RESPONSE}" | grep '"packet":"data"'

echo '--- a preview emptied by HAVING is still delivered'
# A preview replaces the previous one, so a preview that `HAVING` empties must arrive as an empty
# preview instead of being dropped, which would leave the previous rows on the screen.
HAVING_QUERY="SELECT intDiv(number, 250000) AS k, count() AS c FROM numbers(1000000) GROUP BY k HAVING c > 1000000 ORDER BY k FORMAT JSONCompactEachRow"
HAVING_RESPONSE=$(${CLICKHOUSE_CURL} -sS "${URL}${BLOCKS}${PREVIEWS}&framing_output_format=JSONEachPacketString" -d "${HAVING_QUERY}")
HAVING_PREVIEWS=$(echo "${HAVING_RESPONSE}" | grep -c '"packet":"preview"')
if [ "${HAVING_PREVIEWS}" -ge 1 ]; then echo "has previews"; else echo "no previews: ${HAVING_RESPONSE}"; fi
echo "${HAVING_RESPONSE}" | grep '"packet":"preview"' | python3 -c "
import json, sys
ok = True
for line in sys.stdin:
    if json.loads(line)['data'] != '':
        ok = False
print('previews are empty' if ok else 'non-empty previews')
"

echo '--- the state-size threshold in bytes stops previews'
# `query_result_previews_max_result_bytes` is compared with the memory the aggregation really
# holds, which for 100000 keys is megabytes - far above the 1 KiB threshold below and far below
# the 1 GiB one, so the outcome does not depend on timing.
STATE_QUERY="SELECT number % 100000 AS k, count() AS c FROM numbers(2000000) GROUP BY k ORDER BY k LIMIT 3 FORMAT JSONCompactEachRow"
run_state()
{
    ${CLICKHOUSE_CURL} -sS "${URL}${BLOCKS}${PREVIEWS}&framing_output_format=JSONEachPacketString&query_result_previews_max_result_rows=1000000&query_result_previews_max_result_bytes=$1" -d "${STATE_QUERY}"
}
ROOMY=$(run_state 1000000000)
TIGHT=$(run_state 1024)
if [ "$(echo "${ROOMY}" | grep -c '"packet":"preview"')" -ge 1 ]; then echo "has previews below the threshold"; else echo "no previews below the threshold: ${ROOMY}"; fi
if [ "$(echo "${TIGHT}" | grep -c '"packet":"preview"')" -eq 0 ]; then echo "no previews above the threshold"; else echo "previews above the threshold"; fi
if [ "$(echo "${ROOMY}" | grep '"packet":"data"')" == "$(echo "${TIGHT}" | grep '"packet":"data"')" ]; then echo "identical"; else echo "differ"; fi

echo '--- without framing, previews are not emitted into the plain output'
PLAIN_WITH=$(${CLICKHOUSE_CURL} -sS "${URL}${BLOCKS}${PREVIEWS}" -d "${QUERY}")
PLAIN_WITHOUT=$(${CLICKHOUSE_CURL} -sS "${URL}${BLOCKS}" -d "${QUERY}")
if [ "${PLAIN_WITH}" == "${PLAIN_WITHOUT}" ]; then echo "identical"; else echo "differ: ${PLAIN_WITH} vs ${PLAIN_WITHOUT}"; fi
