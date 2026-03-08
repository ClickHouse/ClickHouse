#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -H 'Accept-Encoding: snappy'              "${CLICKHOUSE_URL}&enable_http_compression=1" -d "SELECT 1" --output - | ${CLICKHOUSE_SIMPLE_SNAPPY}

${CLICKHOUSE_CURL} -H 'Accept-Encoding: snappy'              "${CLICKHOUSE_URL}&enable_http_compression=1" -d "SELECT 123987" --output -  | ${CLICKHOUSE_SIMPLE_SNAPPY}

${CLICKHOUSE_CURL} -H 'Accept-Encoding: snappy'              "${CLICKHOUSE_URL}&enable_http_compression=1" -d "select 'Hello world Hello world Hello world'" --output -  | ${CLICKHOUSE_SIMPLE_SNAPPY}

${CLICKHOUSE_CURL} -H 'Accept-Encoding: snappy'              "${CLICKHOUSE_URL}&enable_http_compression=1" -d "MAL_FORMED" --output -  2>&1 | grep -o -q 'Code: 62. DB::Exception: Syntax error:' && echo "OK" || echo "FAIL"

${CLICKHOUSE_CURL} -H 'Accept-Encoding: snappy'              "${CLICKHOUSE_URL}&enable_http_compression=1" -d "SELECT MAL_FORMED" --output - 2>&1 | ${CLICKHOUSE_SIMPLE_SNAPPY}  | grep -o -q 'Code: 47. DB::Exception: Unknown expression identifier' && echo "OK" || echo "FAIL"
