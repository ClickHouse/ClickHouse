#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

RETRIES=5

# do not trust CLICKHOUSE_URL var because it contains randomization settings
CLICKHOUSE_URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"

query="SELECT name, value, changed FROM system.settings WHERE name='output_format_parallel_formatting'"

echo "output_format_parallel_formatting=1"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?output_format_parallel_formatting=1" -d "$query"
echo "output_format_parallel_formatting=0"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?output_format_parallel_formatting=0" -d "$query"
echo "output_format_parallel_formatting=1&output_format_parallel_formatting=0"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?output_format_parallel_formatting=1&output_format_parallel_formatting=0" -d "$query"
echo "output_format_parallel_formatting=0&output_format_parallel_formatting=1"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?output_format_parallel_formatting=0&output_format_parallel_formatting=1" -d "$query"
