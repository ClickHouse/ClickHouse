#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


CURL_OUTPUT=$(echo 'SELECT number FROM numbers(10)' | \
  ${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&max_block_size=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&output_format_parallel_formatting=0" --data-binary @- 2>&1)

ELAPSED_NS_PROGRESS="$(echo "${CURL_OUTPUT}" | \
  grep 'X-ClickHouse-Progress' | \
  awk '{print $3}' | \
  jq -cM '.elapsed_ns | tonumber'
  )"

ELAPSED_NS_SUMMARY="$(echo "${CURL_OUTPUT}" | \
  grep 'X-ClickHouse-Summary' | \
  awk '{print $3}' | \
  jq -cM '.elapsed_ns | tonumber'
  )"


ALL_ARE_NON_ZERO=1
while read -r line; do
  if [ "$line" -eq 0 ]; then
    ALL_ARE_NON_ZERO=0
    break
  fi
done <<< "$ELAPSED_NS_PROGRESS"

if [ "$ALL_ARE_NON_ZERO" -eq 1 ] && [ "$(echo "$ELAPSED_NS_SUMMARY" | wc -l)" -gt 0 ]; then
  echo "elapsed_ns in progress are all non zero"
else
  echo "elapsed_ns in progress are all zero!"
fi

if [ "$ELAPSED_NS_SUMMARY" -ne 0 ];
then
  echo "elapsed_ns in summary is not zero"
else
  echo "elapsed_ns in summary is zero!"
fi
