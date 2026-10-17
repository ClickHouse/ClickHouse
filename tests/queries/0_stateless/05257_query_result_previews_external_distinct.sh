#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Query result previews (the `query_result_previews` setting) pass through the final `DISTINCT` that can
# spill to disk (`ExternalDistinctTransform`, the default whenever an external `DISTINCT` threshold applies):
# each preview is deduplicated standalone, and the result is the same as without previews.

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"
PREVIEWS="&query_result_previews=1&query_result_previews_min_interval_ms=0"
# Pin the settings that determine the number of source blocks, so that enough blocks are consumed
# by the aggregation to emit previews regardless of the settings randomization in CI.
BLOCKS="&max_block_size=65536&max_threads=4&group_by_two_level_threshold=100000000&group_by_two_level_threshold_bytes=1000000000"
QUERY="SELECT DISTINCT c FROM (SELECT intDiv(number, 250000) AS k, count() AS c FROM numbers(1000000) GROUP BY k)"

# `threshold` 10 GiB never spills; 1 byte spills at the first real chunk, after the previews.
for threshold in 10737418240 1; do
    EXTERNAL="&max_bytes_before_external_distinct=${threshold}&max_bytes_ratio_before_external_distinct=0"
    echo "--- max_bytes_before_external_distinct = ${threshold}"

    # The step is only meaningful if the final `DISTINCT` is the external one.
    ${CLICKHOUSE_CURL} -sS "${URL}${EXTERNAL}" -d "EXPLAIN PIPELINE ${QUERY}" | grep -o 'ExternalDistinctTransform' | head -n 1

    RESPONSE=$(${CLICKHOUSE_CURL} -sS "${URL}${BLOCKS}${PREVIEWS}${EXTERNAL}&framing_output_format=JSONEachPacketString" -d "${QUERY} FORMAT JSONCompactEachRow")
    NUM_PREVIEWS=$(echo "${RESPONSE}" | grep -c '"packet":"preview"')
    if [ "${NUM_PREVIEWS}" -ge 1 ]; then echo "has previews"; else echo "no previews: ${RESPONSE}"; fi
    echo "${RESPONSE}" | grep '"packet":"preview"' | python3 -c "
import json, sys
ok = True
for line in sys.stdin:
    rows = json.loads(line)['data'].rstrip('\n').split('\n')
    if len(rows) != len(set(rows)):
        ok = False
print('previews are deduplicated' if ok else 'duplicate rows in a preview')
"
    echo "${RESPONSE}" | grep '"packet":"data"'
done
