#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query result preview (the `query_result_previews` setting) of an aggregation is a snapshot of the
# whole aggregation, merged from all of its threads. With `ORDER BY ... LIMIT` after `GROUP BY`, the
# aggregation result is spread over several parallel sorting streams, so the preview chunk travels
# through only one of them: it must still be sorted and cut to the limit as a whole, and then pass
# the final merge untouched, so that every preview is the top-N of the whole snapshot.
# Every source block contains all the keys, so the top-N keys of every preview are known in advance.

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"
SETTINGS="&query_result_previews=1&query_result_previews_min_interval_ms=0&query_result_previews_min_rows=1"
SETTINGS="${SETTINGS}&max_block_size=65536&max_threads=4&group_by_two_level_threshold=100000000&group_by_two_level_threshold_bytes=1000000000"
SETTINGS="${SETTINGS}&max_bytes_before_external_group_by=0&max_bytes_ratio_before_external_group_by=0&enable_adaptive_aggregator=0"
SETTINGS="${SETTINGS}&max_bytes_before_external_sort=0&max_bytes_ratio_before_external_sort=0&output_format_json_quote_64bit_integers=0"

QUERY="SELECT number % 1000 AS k, count() AS c FROM numbers(10000000) GROUP BY k ORDER BY k DESC LIMIT 5 FORMAT JSONCompactEachRow"

RESPONSE=$(${CLICKHOUSE_CURL} -sS "${URL}${SETTINGS}&framing_output_format=JSONEachPacketString" -d "${QUERY}")

echo '--- previews are emitted'
PREVIEWS_COUNT=$(echo "${RESPONSE}" | grep -c '"packet":"preview"')
if [ "${PREVIEWS_COUNT}" -ge 1 ]; then echo "has previews"; else echo "no previews: ${RESPONSE}"; fi

echo '--- every preview is the top-N of the whole aggregation snapshot'
echo "${RESPONSE}" | grep '"packet":"preview"' | python3 -c "
import json, sys
bad = []
for line in sys.stdin:
    packet = json.loads(line)
    rows = [json.loads(row_line) for row_line in packet['data'].rstrip('\n').split('\n')]
    keys = [int(row[0]) for row in rows]
    if keys != [999, 998, 997, 996, 995] or not all(0 < int(row[1]) <= 10000 for row in rows):
        bad.append(rows)
print('ok' if not bad else 'unexpected previews: ' + str(bad[:3]))
"

echo '--- the result'
echo "${RESPONSE}" | grep '"packet":"data"' | python3 -c "
import json, sys
for line in sys.stdin:
    sys.stdout.write(json.loads(line)['data'])
"
