#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_URL="${CLICKHOUSE_URL}&http_wait_end_of_query=1"

RETRIES=5

result=""
lines_expected=5
counter=0
while [ $counter -lt $RETRIES ] && [ "$(echo "$result" | wc -l)" != "$lines_expected" ]; do
    result=$(${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&output_format_parallel_formatting=0&max_block_size=5&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0" -d 'SELECT max(number) FROM numbers(10)' 2>&1 | grep -E 'Content-Encoding|X-ClickHouse-Progress|X-ClickHouse-Summary|^[0-9]' | grep -v 'Access-Control-Expose-Headers' | sed 's/,\"elapsed_ns[^}]*//')
    let counter=counter+1
done
echo "&output_format_parallel_formatting=0&max_block_size=5&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0"
echo "$result"

result=""
lines_expected=5
counter=0
while [ $counter -lt $RETRIES ] && [ "$(echo "$result" | wc -l)" != "$lines_expected" ]; do
    result=$(${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&output_format_parallel_formatting=1&max_block_size=5&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0" -d 'SELECT max(number) FROM numbers(10)' 2>&1 | grep -E 'Content-Encoding|X-ClickHouse-Progress|X-ClickHouse-Summary|^[0-9]' | grep -v 'Access-Control-Expose-Headers' | sed 's/,\"elapsed_ns[^}]*//')
    let counter=counter+1
done
echo "&output_format_parallel_formatting=1&max_block_size=5&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0"
echo "$result"

result=""
lines_expected=22
counter=0
while [ $counter -lt $RETRIES ] && [ "$(echo "$result" | wc -l)" != "$lines_expected" ]; do
    result=$(${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&max_block_size=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0" -d 'SELECT number FROM numbers(10)' 2>&1 | grep -E 'Content-Encoding|X-ClickHouse-Progress|X-ClickHouse-Summary|^[0-9]' | grep -v 'Access-Control-Expose-Headers' | sed 's/,\"elapsed_ns[^}]*//')
    let counter=counter+1
done
echo "&max_block_size=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0"
echo "$result"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&max_block_size=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&enable_http_compression=1" -H 'Accept-Encoding: gzip' -d 'SELECT number FROM system.numbers LIMIT 10' | gzip -d

# 'send_progress_in_http_headers' is false by default
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&max_block_size=1&http_headers_progress_interval_ms=0" -d 'SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep -q 'X-ClickHouse-Progress' && echo 'Fail'

# have header?
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&max_block_size=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&enable_http_compression=1" -H 'Accept-Encoding: gzip' -d 'SELECT number FROM system.numbers LIMIT 1' 2>&1 | grep -q "Content-Encoding: gzip" || echo 'Fail'

# nothing in body = no gzip
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&max_block_size=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&enable_http_compression=1" -H 'Accept-Encoding: gzip' -d 'SELECT number FROM system.numbers LIMIT 0' 2>&1 | grep -q 'Content-Encoding: gzip' && echo 'Fail'

# test insertion stats
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" -H 'Accept-Encoding: gzip' -d 'DROP TABLE IF EXISTS insert_number_query' > /dev/null 2>&1
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" -H 'Accept-Encoding: gzip' -d 'DROP TABLE IF EXISTS insert_number_query_2' > /dev/null 2>&1

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" -H 'Accept-Encoding: gzip' -d 'CREATE TABLE insert_number_query (record UInt32) Engine = Memory' > /dev/null 2>&1
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" -H 'Accept-Encoding: gzip' -d 'CREATE TABLE insert_number_query_2 (record UInt32) Engine = Memory' > /dev/null 2>&1

result=""
counter=0
while [ $counter -lt $RETRIES ] && [ -z "$result" ]; do
    result=$(${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&max_block_size=1&http_headers_progress_interval_ms=0&send_progress_in_http_headers=1" -d 'INSERT INTO insert_number_query (record) SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep -E 'Content-Encoding|X-ClickHouse-Summary|^[0-9]' | grep -v 'Access-Control-Expose-Headers' | sed 's/,\"elapsed_ns[^}]*//')
    let counter=counter+1
done
echo "$result"

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" -H 'Accept-Encoding: gzip' -d 'DROP TABLE insert_number_query' > /dev/null 2>&1
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" -H 'Accept-Encoding: gzip' -d 'DROP TABLE insert_number_query_2' > /dev/null 2>&1
