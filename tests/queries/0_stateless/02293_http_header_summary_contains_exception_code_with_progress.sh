#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


CURL_OUTPUT=$(echo 'SELECT 1 + sleepEachRow(0.00002) FROM numbers(100000)' | \
  ${CLICKHOUSE_CURL_COMMAND} -v "${CLICKHOUSE_URL}&http_wait_end_of_query=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&max_execution_time=1" --data-binary @- 2>&1)

EXCEPTION=$(echo "${CURL_OUTPUT}" | grep 'X-ClickHouse-Exception-Code')

if [[ "$EXCEPTION" =~ .*"159".* ]];
then
  echo "Expected exception: 159"
else
  echo "Unexpected exception"
  echo "EXCEPTION:"
  echo "'${EXCEPTION}'"
  echo "DATA:"
  echo "$CURL_OUTPUT"
fi
