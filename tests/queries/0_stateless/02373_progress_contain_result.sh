#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo 'SELECT 1 FROM numbers(100)' |
  ${CLICKHOUSE_CURL_COMMAND} -v "${CLICKHOUSE_URL}&wait_end_of_query=1&send_progress_in_http_headers=0" --data-binary @- 2>&1 |
  grep 'X-ClickHouse-Summary'
