#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0" -d @- <<< "insert into function null('_ Int') select * from numbers(5) settings max_block_size=1, max_insert_threads=1" -v |& {
    grep -F -e X-ClickHouse-Progress: -e X-ClickHouse-Summary:  | sed 's/,\"elapsed_ns[^}]*//'
}
