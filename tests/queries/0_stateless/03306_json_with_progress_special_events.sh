#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&default_format=JSONEachRowWithProgress&extremes=1&rows_before_aggregation=1" -d "SELECT (123 + number * 456) % 100 AS k, count() AS c, sum(number) AS s FROM numbers(100) GROUP BY ALL WITH TOTALS ORDER BY ALL LIMIT 10" | grep -v '"progress"'
