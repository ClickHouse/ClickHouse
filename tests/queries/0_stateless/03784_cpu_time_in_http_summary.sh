#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Ensure HTTP summary headers include CPU time (value normalized to placeholder).
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&http_wait_end_of_query=1" -d "SELECT 1" -v 2>&1 |
  grep 'X-ClickHouse-Summary' |
  grep -o '"cpu_time_us":"[0-9]*"' |
  sed -E 's/[0-9]+/<num>/'
