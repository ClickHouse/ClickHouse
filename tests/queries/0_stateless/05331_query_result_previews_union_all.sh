#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query result preview (the `query_result_previews` setting) is a snapshot of one preview-emitting
# stage and fully replaces the previous preview. When the result is produced by several independent
# branches, such as `UNION ALL` of two aggregations, or an aggregation and a branch without previews,
# a preview of one branch would be shown in place of the whole result, so previews stay dormant.

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"
SETTINGS="&query_result_previews=1&query_result_previews_min_interval_ms=0&query_result_previews_min_rows=1"
SETTINGS="${SETTINGS}&max_block_size=65536&max_threads=4&group_by_two_level_threshold=100000000&group_by_two_level_threshold_bytes=1000000000"
SETTINGS="${SETTINGS}&max_bytes_before_external_group_by=0&max_bytes_ratio_before_external_group_by=0&enable_adaptive_aggregator=0"

count_previews()
{
    ${CLICKHOUSE_CURL} -sS "${URL}${SETTINGS}&framing_output_format=JSONEachPacketString" -d "$1 FORMAT JSONCompactEachRow" | grep -c '"packet":"preview"'
}

check()
{
    if [ "$(count_previews "$1")" -ge 1 ]; then echo "has previews"; else echo "no previews"; fi
}

echo '--- a single aggregation'
check "SELECT count() FROM numbers(10000000)"

echo '--- UNION ALL of two aggregations'
check "SELECT count() FROM numbers(10000000) UNION ALL SELECT count() FROM numbers(10000000)"

echo '--- UNION ALL of an aggregation and a branch without previews'
check "SELECT count() FROM numbers(10000000) UNION ALL SELECT 1"

echo '--- the result of UNION ALL'
${CLICKHOUSE_CURL} -sS "${URL}${SETTINGS}" -d "SELECT * FROM (SELECT count() FROM numbers(10000000) UNION ALL SELECT sum(number) FROM numbers(10000000)) ORDER BY 1"
