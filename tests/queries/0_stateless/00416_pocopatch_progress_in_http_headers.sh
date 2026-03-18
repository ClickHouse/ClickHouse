#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

RETRIES=5

result=""
lines_expected=4
counter=0
while [ $counter -lt $RETRIES ] && [ "$(echo "$result" | wc -l)" != "$lines_expected" ]; do
    result=$(${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&max_block_size=5&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0" -d 'SELECT max(number) FROM numbers(10)' 2>&1 | grep -E 'Content-Encoding|X-ClickHouse-Progress|^[0-9]' | sed 's/,\"elapsed_ns[^}]*//')
    let counter=counter+1
done
echo "$result"

result=""
lines_expected=12
counter=0
while [ $counter -lt $RETRIES ] && [ "$(echo "$result" | wc -l)" != "$lines_expected" ]; do
    result=$(${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&max_block_size=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&output_format_parallel_formatting=0" -d 'SELECT number FROM numbers(10)' 2>&1 | grep -E 'Content-Encoding|X-ClickHouse-Progress|^[0-9]'| sed 's/,\"elapsed_ns[^}]*//')
    let counter=counter+1
done
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
    result=$(${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&max_block_size=1&http_headers_progress_interval_ms=0&send_progress_in_http_headers=1" -d 'INSERT INTO insert_number_query (record) SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep -E 'Content-Encoding|X-ClickHouse-Summary|^[0-9]' | sed 's/,\"elapsed_ns[^}]*//')
    let counter=counter+1
done
echo "$result"

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" -H 'Accept-Encoding: gzip' -d 'DROP TABLE insert_number_query' > /dev/null 2>&1
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" -H 'Accept-Encoding: gzip' -d 'DROP TABLE insert_number_query_2' > /dev/null 2>&1
