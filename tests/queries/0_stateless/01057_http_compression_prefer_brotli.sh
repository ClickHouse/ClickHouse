#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: br'              "${CLICKHOUSE_URL}&enable_http_compression=1" -d 'SELECT 1' | brotli -d
${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: br,gzip'         "${CLICKHOUSE_URL}&enable_http_compression=1" -d 'SELECT 1' | brotli -d
${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip,br'         "${CLICKHOUSE_URL}&enable_http_compression=1" -d 'SELECT 1' | brotli -d
${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip,deflate,br' "${CLICKHOUSE_URL}&enable_http_compression=1" -d 'SELECT 1' | brotli -d
${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip,deflate'    "${CLICKHOUSE_URL}&enable_http_compression=1" -d 'SELECT 1' | gzip -d
${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip'            "${CLICKHOUSE_URL}&enable_http_compression=1" -d 'SELECT number FROM numbers(1000000)' | gzip -d | tail -n3
${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: br'              "${CLICKHOUSE_URL}&enable_http_compression=1" -d 'SELECT number FROM numbers(1000000)' | brotli -d | tail -n3

${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: br'              "${CLICKHOUSE_URL}&enable_http_compression=1" -d "SELECT toDate('2020-12-12') as datetime, 'test-pipeline' as pipeline, 'clickhouse-test-host-001.clickhouse.com' as host, 'clickhouse' as home, 'clickhouse' as detail, number as row_number FROM numbers(1000000) FORMAT JSON" | brotli -d | tail -n30 | head -n23
