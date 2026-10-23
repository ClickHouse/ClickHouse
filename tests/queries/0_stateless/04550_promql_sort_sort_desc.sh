#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the PromQL grammar requires ANTLR4 which is disabled in the fast-test build.
# no-replicated-database: the experimental TimeSeries table engine does not round-trip through DatabaseReplicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -m -q "
CREATE TABLE ts (samples Array(Tuple(DateTime64(3, 'UTC'), Float64))) ENGINE = TimeSeries;

-- Insert 3 series with values 30, 10, 20 (deliberately unsorted).
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('up', {'instance':'host1'}, [(toDateTime64(1700000000, 3, 'UTC'), 30)]),
    ('up', {'instance':'host2'}, [(toDateTime64(1700000000, 3, 'UTC'), 10)]),
    ('up', {'instance':'host3'}, [(toDateTime64(1700000000, 3, 'UTC'), 20)]);
"

promql_client()
{
    $CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 "$@"
}

echo "-- sort(up): ascending by value (10, 20, 30)"
promql_client -q "sort(up)"

echo "-- sort_desc(up): descending by value (30, 20, 10)"
promql_client -q "sort_desc(up)"

echo "-- sort(sort_desc(up)): ascending again (cancels out)"
promql_client -q "sort(sort_desc(up))"

echo "-- sort on expression: sort(up * 2) ascending (20, 40, 60)"
promql_client -q "sort(up * 2)"

echo "-- sort_desc(sort_by_label(up, 'instance')): sort() must override the ordering mode entirely,"
echo "-- so this is descending by value (30, 20, 10), not descending by the 'instance' label"
promql_client -q "sort_desc(sort_by_label(up, 'instance'))"

echo "-- -sort(up): unary minus keeps the order fixed by sort() (10, 20, 30 before negation)"
promql_client -q "-sort(up)"

echo "-- abs(sort(up - 25)): abs() keeps the order fixed by sort() (-15, -5, 5 before abs)"
promql_client -q "abs(sort(up - 25))"

echo "-- timestamp(sort_desc(up)): timestamp() keeps the order fixed by sort_desc() (30, 20, 10 before timestamp)"
promql_client -q "timestamp(sort_desc(up))"

echo "-- label_replace(sort_desc(up), ...): label changes keep the order fixed by sort_desc()"
promql_client -q "label_replace(sort_desc(up), 'note', 'x', 'instance', '.*')"

echo "-- topk(2, sort(up)): the outer topk() orders its result by value (30, 20), not by the inner sort()"
promql_client -q "topk(2, sort(up))"

echo "-- sort(topk(2, up)): the outer sort() orders the result of topk() ascending (20, 30)"
promql_client -q "sort(topk(2, up))"

echo "-- sort(up) and up: 'and' keeps the order fixed by sort() on its left side (10, 20, 30)"
promql_client -q "sort(up) and up"

echo "-- sort_desc(up) unless up{instance='host3'}: 'unless' keeps the order fixed by sort_desc() on its left side (30, 10)"
promql_client -q "sort_desc(up) unless up{instance='host3'}"

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -m -q "
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('a', {'job':'x'}, [(toDateTime64(1700000000, 3, 'UTC'), 3)]),
    ('b', {'job':'x'}, [(toDateTime64(1700000000, 3, 'UTC'), 10)]),
    ('c', {'job':'y'}, [(toDateTime64(1700000000, 3, 'UTC'), 7)]);
"

echo "-- (sort(a|b|c) and b|c) * 1: a series removed by 'and' does not keep its rank after the metric name is dropped (y=7, x=10)"
promql_client -q "(sort({__name__=~'a|b|c'}) and on(__name__) {__name__=~'b|c'}) * 1"

echo "-- label_replace() of the same: the series removed by 'and' does not keep its rank either (y=7, x=10)"
promql_client -q "label_replace(sort({__name__=~'a|b|c'}) and on(__name__) {__name__=~'b|c'}, '__name__', 'm', '', '')"

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -m -q "
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('other', {'instance':'host1'}, [(toDateTime64(1700000000, 3, 'UTC'), 1)]),
    ('other', {'instance':'host2'}, [(toDateTime64(1700000000, 3, 'UTC'), 2)]),
    ('other', {'instance':'host3'}, [(toDateTime64(1700000000, 3, 'UTC'), 3)]);
"

echo "-- sort(up) + other: a binary operator keeps the order fixed by sort() on its left side (12, 23, 31)"
promql_client -q "sort(up) + other"

echo "-- sort(up) > other: a comparison keeps the order fixed by sort() on its left side (10, 20, 30)"
promql_client -q "sort(up) > other"

echo "-- other * on(instance) group_right sort_desc(up): group_right keeps the order of its right side (30, 60, 20)"
promql_client -q "other * on(instance) group_right sort_desc(up)"

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts"
