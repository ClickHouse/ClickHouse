#!/usr/bin/env bash
# Tags: long, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: gzip'              "${CLICKHOUSE_URL}&enable_http_compression=1&http_zlib_compression_level=1" -d "SELECT toDate('2020-12-12') as datetime, 'test-pipeline' as pipeline, 'clickhouse-test-host-001.clickhouse.com' as host, 'clickhouse' as home, 'clickhouse' as detail, number as row_number FROM numbers(100000) FORMAT JSON" | gzip -d | tail -n30 | head -n23
