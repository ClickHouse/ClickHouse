#!/usr/bin/env bash
# Tags: no-parallel-replicas

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

# The total is known before the query starts reading, so the very FIRST progress report must
# already carry all 10 rows and no read progress. Zero-valued fields are omitted from the
# progress JSON, so "no read progress" means "read_rows" is absent, not zero.
first_report=$($CLICKHOUSE_CURL -vsS "${CLICKHOUSE_URL}&max_threads=1&default_format=Null&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0" --data-binary @- <<< "select * from data_01882" 2>&1 | grep -o -m1 -E 'X-ClickHouse-Progress: \{[^}]*\}')
echo "$first_report" | grep -q -F '"total_rows_to_read":"10"' && echo 'first report has the total'
echo "$first_report" | grep -q -F '"read_rows"' || echo 'first report has no read progress'

# Reading in order builds one source per part, each carrying only that part's share of the total, so
# the first report is complete only if every share reached it. The table has ten one-row parts and the
# query has no limit, so the whole table is expected.
first_report_in_order=$($CLICKHOUSE_CURL -vsS "${CLICKHOUSE_URL}&max_threads=1&default_format=Null&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&optimize_read_in_order=1" --data-binary @- <<< "select * from data_01882 order by key" 2>&1 | grep -o -m1 -E 'X-ClickHouse-Progress: \{[^}]*\}')
echo "$first_report_in_order" | grep -q -F '"total_rows_to_read":"10"' && echo 'first in-order report has the total'

# A limit bounds the whole read instead of each share of it: the shares stay per part, but they sum to
# the limit rather than to one limit per part. Every part here holds more rows than the limit, so a
# total that grows with the number of parts means the limit reached the estimate more than once.
$CLICKHOUSE_CLIENT -q "drop table if exists data_01882_limit"
$CLICKHOUSE_CLIENT -q "create table data_01882_limit (key Int) engine=MergeTree() partition by intDiv(key, 100) order by key as select * from numbers(400)"

in_order_totals=$($CLICKHOUSE_CURL -vsS "${CLICKHOUSE_URL}&max_threads=1&default_format=Null&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&optimize_read_in_order=1&query_plan_read_in_order=1" --data-binary @- <<< "select key from data_01882_limit order by key limit 10" 2>&1 | grep -o -E '"total_rows_to_read":"[0-9]+"' | sort -u)
[ "$in_order_totals" = '"total_rows_to_read":"10"' ] && echo 'in-order limit is counted once'

trivial_totals=$($CLICKHOUSE_CURL -vsS "${CLICKHOUSE_URL}&default_format=Null&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0" --data-binary @- <<< "select key from data_01882_limit limit 5" 2>&1 | grep -o -E '"total_rows_to_read":"[0-9]+"' | sort -u)
[ "$trivial_totals" = '"total_rows_to_read":"5"' ] && echo 'trivial limit is counted once'
