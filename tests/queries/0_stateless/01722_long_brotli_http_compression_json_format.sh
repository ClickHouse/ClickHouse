#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS -H 'Accept-Encoding: br'              "${CLICKHOUSE_URL}&enable_http_compression=1" -d "SELECT toDate('2020-12-12') as datetime, 'test-pipeline' as pipeline, 'clickhouse-test-host-001.clickhouse.com' as host, 'clickhouse' as home, 'clickhouse' as detail, number as row_number FROM numbers(1000000) FORMAT JSON" | brotli -d | tail -n30 | head -n23
