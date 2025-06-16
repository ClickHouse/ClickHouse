#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

(
    echo "insert into function null('_ Int') format TSV"

    echo

    for x in {1..3}; do
        for y in {1..1000000}; do
            echo $y
        done
        sleep 1
    done
    sleep 1
) | ${CLICKHOUSE_CURL} -v -S -X POST "${CLICKHOUSE_URL}&send_progress_in_http_headers=1&http_headers_progress_interval_ms=100&max_threads=1&max_insert_threads=1&max_block_size=1&max_insert_block_size=1" --data-binary @- |& {
    grep -F -e X-ClickHouse-Progress: -e X-ClickHouse-Summary: -e Connection: | sed 's/: {.*}//' | uniq
}
