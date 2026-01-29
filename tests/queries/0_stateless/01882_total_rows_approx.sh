#!/usr/bin/env bash

# Check that total_rows_approx (via http headers) includes all rows from
# all parts at the query start.
#
# At some point total_rows_approx was accounted only when the query starts
# reading the part, and so total_rows_approx wasn't reliable, even for simple
# SELECT FROM MergeTree()
# It was fixed by take total_rows_approx into account as soon as possible.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "drop table if exists data_01882"
$CLICKHOUSE_CLIENT -q "create table data_01882 (key Int) Engine=MergeTree() partition by key order by key as select * from numbers(10)"
# send_progress_in_http_headers will periodically send the progress
# but this is not stable, i.e. it can be dumped on query end,
# thus check few times to be sure that this is not coincidence.
for _ in {1..30}; do
    $CLICKHOUSE_CURL -vsS "${CLICKHOUSE_URL}&max_threads=1&default_format=Null&send_progress_in_http_headers=1&http_headers_progress_interval_ms=1" --data-binary @- <<< "select * from data_01882" |& {
        grep -o -F '"total_rows_to_read":"10"'
    } | {
        # grep out final result
        grep -v -F '"read_rows":"10"'
    }
done | uniq
