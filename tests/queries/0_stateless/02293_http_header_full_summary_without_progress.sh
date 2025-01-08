#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


# Sanity check to ensure that the server is up and running
for _ in {1..10}; do
    echo 'SELECT 1' | ${CLICKHOUSE_CURL_COMMAND} -s "${CLICKHOUSE_URL}" --data-binary @- > /dev/null
    if [ $? -eq 0 ]; then
        break
    fi
    sleep 1
done

CURL_OUTPUT=$(echo 'SELECT 1 + sleepEachRow(0.00002) FROM numbers(100000)' | \
  ${CLICKHOUSE_CURL_COMMAND} --max-time 3 -vsS "${CLICKHOUSE_URL}&wait_end_of_query=1&send_progress_in_http_headers=0&max_execution_time=1" --data-binary @- 2>&1)

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
  echo "${CURL_OUTPUT}"
fi

# Check that the response code is correct too
echo "${CURL_OUTPUT}" | grep "< HTTP/1.1"
