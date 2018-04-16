#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# This test will fail with external poco (progress not supported)

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}?max_block_size=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0" -d 'SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep -E 'Content-Encoding|X-ClickHouse-Progress|^[0-9]'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?max_block_size=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&enable_http_compression=1" -H 'Accept-Encoding: gzip' -d 'SELECT number FROM system.numbers LIMIT 10' | gzip -d

# 'send_progress_in_http_headers' is false by default
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}?max_block_size=1&http_headers_progress_interval_ms=0" -d 'SELECT number FROM system.numbers LIMIT 10' 2>&1 | grep -q 'X-ClickHouse-Progress' && echo 'Fail' || true

# have header?
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}?max_block_size=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&enable_http_compression=1" -H 'Accept-Encoding: gzip' -d 'SELECT number FROM system.numbers LIMIT 1' 2>&1 | grep -q "Content-Encoding: gzip" && true || echo 'Fail'

# nothing in body = no gzip
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}?max_block_size=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&enable_http_compression=1" -H 'Accept-Encoding: gzip' -d 'SELECT number FROM system.numbers LIMIT 0' 2>&1 | grep -q 'Content-Encoding: gzip' && echo 'Fail' || true
