#!/usr/bin/env bash

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/32186

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}/?&query=SELECT+cluster%2C+host_address%2C+port+FROM+system.clusters+FORMAT+JSON&max_result_bytes=104857600&log_queries=1&optimize_throw_if_noop=1&output_format_json_quote_64bit_integers=0&lock_acquire_timeout=10&max_execution_time=10&wait_end_of_query=1" >/dev/null
