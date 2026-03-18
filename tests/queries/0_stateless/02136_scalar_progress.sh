#!/usr/bin/env bash
# Ref: https://github.com/ClickHouse/ClickHouse/issues/1576
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&wait_end_of_query=1&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0" -d "SELECT (SELECT max(number), count(number) FROM numbers(100000) settings max_block_size=65505);" -v 2>&1 | grep -E "X-ClickHouse-Summary|X-ClickHouse-Progress" | sed 's/,\"elapsed_ns[^}]*//'
