#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


CURL_OUTPUT=$(echo 'SELECT 1 + sleepEachRow(0.00002) FROM numbers(100000)' | \
  ${CLICKHOUSE_CURL_COMMAND} -v "${CLICKHOUSE_URL}&wait_end_of_query=1&send_progress_in_http_headers=0&max_execution_time=1" --data-binary @- 2>&1)

READ_ROWS=$(echo "${CURL_OUTPUT}" | \
  grep 'X-ClickHouse-Summary' | \
  awk '{print $3}' | \
  sed -E 's/.*"read_rows":"?([^,"]*)"?.*/\1/'
  )

if [ "$READ_ROWS" -ne 0 ];
then
  echo "Read rows in summary is not zero"
else
  echo "Read rows in summary is zero!"
fi

# Check that the response code is correct too
echo "${CURL_OUTPUT}" | grep "< HTTP/1.1"
