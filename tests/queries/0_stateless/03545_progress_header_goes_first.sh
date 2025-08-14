#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_URL="${CLICKHOUSE_URL}&max_block_size=10000"

(
    echo "insert into function null('_ Int') format TSV"

    echo

    for x in {1..2}; do
        for y in {1..10000}; do
            echo $y
        done
    done
) | ${CLICKHOUSE_CURL} -v -S -X POST "${CLICKHOUSE_URL}&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0" --data-binary @- |& {
    grep -F -e X-ClickHouse-Progress: -e X-ClickHouse-Summary: -e Connection: | sed 's/: {.*}//' | uniq
}
