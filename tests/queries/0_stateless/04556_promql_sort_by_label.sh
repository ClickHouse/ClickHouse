#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the PromQL grammar requires ANTLR4 which is disabled in the fast-test build.
# no-replicated-database: the experimental TimeSeries table engine does not round-trip through DatabaseReplicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -m -q "
CREATE TABLE ts_data (id UUID, timestamp DateTime64(3, 'UTC'), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE ts_tags (id UUID, metric_name LowCardinality(String), tags Map(LowCardinality(String), String))
ENGINE = ReplacingMergeTree ORDER BY (metric_name, id);
CREATE TABLE ts_time_ranges (
    id UUID,
    min_time SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    max_time SimpleAggregateFunction(max, DateTime64(3, 'UTC')))
ENGINE = AggregatingMergeTree ORDER BY id;
CREATE TABLE ts_metrics (metric_family String, type String, unit String, help String) ENGINE = ReplacingMergeTree ORDER BY metric_family;
CREATE TABLE ts ENGINE = TimeSeries DATA ts_data TAGS ts_tags TIME RANGES ts_time_ranges METRICS ts_metrics;

-- 'up' has a single label 'instance' with values that differ numerically (host1, host2, host10, host20)
-- so that natural (numeric-aware) sorting differs from plain lexicographic sorting.
INSERT INTO ts_tags VALUES
    ('00000000-0000-0000-0000-000000000001', 'up', {'instance':'host1'}),
    ('00000000-0000-0000-0000-000000000002', 'up', {'instance':'host2'}),
    ('00000000-0000-0000-0000-000000000003', 'up', {'instance':'host10'}),
    ('00000000-0000-0000-0000-000000000004', 'up', {'instance':'host20'});

-- 'mem' has two labels 'dc' and 'host' to exercise multi-label ordering and the tiebreak,
-- plus one series that is missing the 'dc' label (treated as an empty string).
INSERT INTO ts_tags VALUES
    ('00000000-0000-0000-0000-000000000011', 'mem', {'dc':'us','host':'b'}),
    ('00000000-0000-0000-0000-000000000012', 'mem', {'dc':'us','host':'a'}),
    ('00000000-0000-0000-0000-000000000013', 'mem', {'dc':'eu','host':'z'}),
    ('00000000-0000-0000-0000-000000000014', 'mem', {'host':'only'});

-- 'net' has label values where a digit run meets punctuation; Prometheus compares the non-digit runs first.
INSERT INTO ts_tags VALUES
    ('00000000-0000-0000-0000-000000000021', 'net', {'instance':'host2'}),
    ('00000000-0000-0000-0000-000000000022', 'net', {'instance':'host-1'}),
    ('00000000-0000-0000-0000-000000000023', 'net', {'instance':'host10'}),
    ('00000000-0000-0000-0000-000000000024', 'net', {'instance':'host.1'}),
    ('00000000-0000-0000-0000-000000000025', 'net', {'instance':'hostA'}),
    ('00000000-0000-0000-0000-000000000026', 'net', {'instance':'host_1'});

-- 'big' has digit runs above the Int64 range, which Prometheus compares as plain strings.
INSERT INTO ts_tags VALUES
    ('00000000-0000-0000-0000-000000000031', 'big', {'instance':'host9999999999999999999'}),
    ('00000000-0000-0000-0000-000000000032', 'big', {'instance':'host10000000000000000000'});
INSERT INTO ts_time_ranges SELECT id, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC') FROM ts_tags;

INSERT INTO ts_data VALUES
    ('00000000-0000-0000-0000-000000000001', toDateTime64(1700000000, 3, 'UTC'), 1),
    ('00000000-0000-0000-0000-000000000002', toDateTime64(1700000000, 3, 'UTC'), 2),
    ('00000000-0000-0000-0000-000000000003', toDateTime64(1700000000, 3, 'UTC'), 10),
    ('00000000-0000-0000-0000-000000000004', toDateTime64(1700000000, 3, 'UTC'), 20),
    ('00000000-0000-0000-0000-000000000011', toDateTime64(1700000000, 3, 'UTC'), 11),
    ('00000000-0000-0000-0000-000000000012', toDateTime64(1700000000, 3, 'UTC'), 12),
    ('00000000-0000-0000-0000-000000000013', toDateTime64(1700000000, 3, 'UTC'), 13),
    ('00000000-0000-0000-0000-000000000014', toDateTime64(1700000000, 3, 'UTC'), 14),
    ('00000000-0000-0000-0000-000000000021', toDateTime64(1700000000, 3, 'UTC'), 21),
    ('00000000-0000-0000-0000-000000000022', toDateTime64(1700000000, 3, 'UTC'), 22),
    ('00000000-0000-0000-0000-000000000023', toDateTime64(1700000000, 3, 'UTC'), 23),
    ('00000000-0000-0000-0000-000000000024', toDateTime64(1700000000, 3, 'UTC'), 24),
    ('00000000-0000-0000-0000-000000000025', toDateTime64(1700000000, 3, 'UTC'), 25),
    ('00000000-0000-0000-0000-000000000026', toDateTime64(1700000000, 3, 'UTC'), 26),
    ('00000000-0000-0000-0000-000000000031', toDateTime64(1700000000, 3, 'UTC'), 31),
    ('00000000-0000-0000-0000-000000000032', toDateTime64(1700000000, 3, 'UTC'), 32);
"

promql_client()
{
    $CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 "$@"
}

echo "-- sort_by_label(up, 'instance'): natural ascending order (host1, host2, host10, host20)"
promql_client -q "sort_by_label(up, 'instance')"

echo "-- sort_by_label_desc(up, 'instance'): natural descending order (host20, host10, host2, host1)"
promql_client -q "sort_by_label_desc(up, 'instance')"

echo "-- sort_by_label(mem, 'dc', 'host'): sort by 'dc' then 'host'; missing 'dc' sorts first (empty string)"
promql_client -q "sort_by_label(mem, 'dc', 'host')"

echo "-- sort_by_label_desc(mem, 'dc', 'host'): reverse of the above"
promql_client -q "sort_by_label_desc(mem, 'dc', 'host')"

echo "-- sort_by_label(net, 'instance'): Prometheus order (host2, host10, host-1, host.1, hostA, host_1)"
promql_client -q "sort_by_label(net, 'instance')"

echo "-- sort_by_label_desc(net, 'instance'): reverse of the above"
promql_client -q "sort_by_label_desc(net, 'instance')"

echo "-- sort_by_label(big, 'instance'): host10000000000000000000 first, as in Prometheus"
promql_client -q "sort_by_label(big, 'instance')"

echo "-- sort_by_label_desc(big, 'instance'): reverse of the above"
promql_client -q "sort_by_label_desc(big, 'instance')"

echo "-- abs(sort_by_label(mem, 'host')): abs() keeps the order fixed by sort_by_label()"
promql_client -q "abs(sort_by_label(mem, 'host'))"

echo "-- label_replace(sort_by_label_desc(up, 'instance'), ...): label changes keep the order"
promql_client -q "label_replace(sort_by_label_desc(up, 'instance'), 'zone', 'z', 'instance', '.*')"

echo "-- sum by (instance) (sort(up)): aggregation after sort() must not reuse sort()'s stale rank map"
promql_client -q "sum by (instance) (sort(up))" | LC_ALL=C sort

echo "-- quantile(0.5, sort(up)): same for the quantile aggregation operator; median of 1,2,10,20 is 6"
promql_client -q "quantile(0.5, sort(up))"

echo "-- vector(scalar(sort(up))): same through scalar(); more than one series makes scalar() nan"
promql_client -q "vector(scalar(sort(up)))"

echo "-- error: sort_by_label requires at least 2 arguments"
promql_client -q "sort_by_label(up)" 2>&1 | grep -o "expects at least 2 arguments" | head -n 1

echo "-- error: label arguments must be strings"
promql_client -q "sort_by_label(up, 5)" 2>&1 | grep -o "of type STRING" | head -n 1

echo "-- error: sort_by_label's first argument must be an instant vector"
promql_client -q "sort_by_label(1, 'x')" 2>&1 | grep -o "expects the first argument of type" | head -n 1

echo "-- error: sort takes exactly 1 argument"
promql_client -q "sort()" 2>&1 | grep -o "expects 1 argument" | head -n 1

echo "-- error: sort's argument must be an instant vector"
promql_client -q "sort(1)" 2>&1 | grep -o "expects an argument of type" | head -n 1

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_data"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_tags"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_time_ranges"
$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts_metrics"
