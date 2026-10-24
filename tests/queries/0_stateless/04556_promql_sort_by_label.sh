#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, long
# no-fasttest: the PromQL grammar requires ANTLR4 which is disabled in the fast-test build.
# no-replicated-database: the experimental TimeSeries table engine does not round-trip through DatabaseReplicated.
# long: about 40 PromQL queries, which take more than 180s in the flaky check.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -m -q "
CREATE TABLE ts (samples Array(Tuple(DateTime64(3, 'UTC'), Float64))) ENGINE = TimeSeries;

-- 'up' has a single label 'instance' with values that differ numerically (host1, host2, host10, host20)
-- so that natural (numeric-aware) sorting differs from plain lexicographic sorting.
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('up', {'instance':'host1'}, [(toDateTime64(1700000000, 3, 'UTC'), 1)]),
    ('up', {'instance':'host2'}, [(toDateTime64(1700000000, 3, 'UTC'), 2)]),
    ('up', {'instance':'host10'}, [(toDateTime64(1700000000, 3, 'UTC'), 10)]),
    ('up', {'instance':'host20'}, [(toDateTime64(1700000000, 3, 'UTC'), 20)]);

-- 'mem' has two labels 'dc' and 'host' to exercise multi-label ordering and the tiebreak,
-- plus one series that is missing the 'dc' label (treated as an empty string).
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('mem', {'dc':'us','host':'b'}, [(toDateTime64(1700000000, 3, 'UTC'), 11)]),
    ('mem', {'dc':'us','host':'a'}, [(toDateTime64(1700000000, 3, 'UTC'), 12)]),
    ('mem', {'dc':'eu','host':'z'}, [(toDateTime64(1700000000, 3, 'UTC'), 13)]),
    ('mem', {'host':'only'}, [(toDateTime64(1700000000, 3, 'UTC'), 14)]);

-- 'net' has label values where a digit run meets punctuation; Prometheus compares the non-digit runs first.
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('net', {'instance':'host2'}, [(toDateTime64(1700000000, 3, 'UTC'), 21)]),
    ('net', {'instance':'host-1'}, [(toDateTime64(1700000000, 3, 'UTC'), 22)]),
    ('net', {'instance':'host10'}, [(toDateTime64(1700000000, 3, 'UTC'), 23)]),
    ('net', {'instance':'host.1'}, [(toDateTime64(1700000000, 3, 'UTC'), 24)]),
    ('net', {'instance':'hostA'}, [(toDateTime64(1700000000, 3, 'UTC'), 25)]),
    ('net', {'instance':'host_1'}, [(toDateTime64(1700000000, 3, 'UTC'), 26)]);

-- 'big' has digit runs above the Int64 range, which Prometheus compares as plain strings.
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('big', {'instance':'host9999999999999999999'}, [(toDateTime64(1700000000, 3, 'UTC'), 31)]),
    ('big', {'instance':'host10000000000000000000'}, [(toDateTime64(1700000000, 3, 'UTC'), 32)]);

-- 'cyc' has digit runs on both sides of the Int64 limit: Prometheus's comparator orders them in a cycle,
-- so only Prometheus's own sort algorithm over the tag-sorted input gives its order.
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('cyc', {'instance':'host99'}, [(toDateTime64(1700000000, 3, 'UTC'), 41)]),
    ('cyc', {'instance':'host923'}, [(toDateTime64(1700000000, 3, 'UTC'), 42)]),
    ('cyc', {'instance':'host9223372036854775807'}, [(toDateTime64(1700000000, 3, 'UTC'), 43)]),
    ('cyc', {'instance':'host9223372036854775808'}, [(toDateTime64(1700000000, 3, 'UTC'), 44)]),
    ('cyc', {'instance':'host10000000000000000000'}, [(toDateTime64(1700000000, 3, 'UTC'), 45)]);

-- 'lz' has equal numbers written with different leading zeros; Prometheus's comparator calls each one smaller.
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('lz', {'instance':'host01'}, [(toDateTime64(1700000000, 3, 'UTC'), 51)]),
    ('lz', {'instance':'host1'}, [(toDateTime64(1700000000, 3, 'UTC'), 52)]),
    ('lz', {'instance':'host001'}, [(toDateTime64(1700000000, 3, 'UTC'), 53)]);
"

promql_client()
{
    $CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 "$@"
}

echo "-- sort_by_label(up, 'instance'): natural ascending order (host1, host2, host10, host20)"
promql_client -q "sort_by_label(up, 'instance')"

echo "-- sort_by_label_desc(up, 'instance'): natural descending order (host20, host10, host2, host1)"
promql_client -q "sort_by_label_desc(up, 'instance')"

echo "-- sort_by_label(mem, 'dc', 'host'): sort by 'dc' then 'host'; as in Prometheus, the series without 'dc' ends up last"
promql_client -q "sort_by_label(mem, 'dc', 'host')"

echo "-- sort_by_label_desc(mem, 'dc', 'host'): reverse of the above, the series without 'dc' first"
promql_client -q "sort_by_label_desc(mem, 'dc', 'host')"

echo "-- sort_by_label(net, 'instance'): Prometheus order (host2, host10, host-1, host.1, hostA, host_1)"
promql_client -q "sort_by_label(net, 'instance')"

echo "-- sort_by_label_desc(net, 'instance'): reverse of the above"
promql_client -q "sort_by_label_desc(net, 'instance')"

echo "-- sort_by_label(big, 'instance'): host10000000000000000000 first, as in Prometheus"
promql_client -q "sort_by_label(big, 'instance')"

echo "-- sort_by_label_desc(big, 'instance'): reverse of the above"
promql_client -q "sort_by_label_desc(big, 'instance')"

echo "-- sort_by_label(cyc, 'instance'): the order of Prometheus 3.5"
promql_client -q "sort_by_label(cyc, 'instance')"

echo "-- sort_by_label_desc(cyc, 'instance'): the order of Prometheus 3.5"
promql_client -q "sort_by_label_desc(cyc, 'instance')"

echo "-- sort_by_label(lz, 'instance'): the order of Prometheus 3.5"
promql_client -q "sort_by_label(lz, 'instance')"

echo "-- sort_by_label_desc(lz, 'instance'): the order of Prometheus 3.5"
promql_client -q "sort_by_label_desc(lz, 'instance')"

echo "-- sort_by_label(up): no labels sorts by all labels, as in Prometheus"
promql_client -q "sort_by_label(up)"

echo "-- sort_by_label_desc(up): reverse of the above"
promql_client -q "sort_by_label_desc(up)"

echo "-- sort_by_label(mem): the series without 'dc' ends up last"
promql_client -q "sort_by_label(mem)"

echo "-- sort_by_label_desc(mem): reverse of the above"
promql_client -q "sort_by_label_desc(mem)"

echo "-- sort_by_label_desc(sort_by_label(lz, 'instance'), 'instance'): the outer sort starts from the inner order, as in Prometheus 3.5"
promql_client -q "sort_by_label_desc(sort_by_label(lz, 'instance'), 'instance')"

echo "-- sort_by_label(sort_by_label(lz, 'instance'), 'instance'): the order of Prometheus 3.5"
promql_client -q "sort_by_label(sort_by_label(lz, 'instance'), 'instance')"

echo "-- sort_by_label(sort_by_label_desc(lz, 'instance'), 'instance'): the order of Prometheus 3.5"
promql_client -q "sort_by_label(sort_by_label_desc(lz, 'instance'), 'instance')"

echo "-- sort_by_label(sort(lz), 'instance'): the order of Prometheus 3.5"
promql_client -q "sort_by_label(sort(lz), 'instance')"

echo "-- sort_by_label(abs(sort_desc(lz)), 'instance'): the order of Prometheus 3.5"
promql_client -q "sort_by_label(abs(sort_desc(lz)), 'instance')"

echo "-- sort_by_label(cyc < 44, 'instance'): series filtered out do not take part in sorting, as in Prometheus 3.5"
promql_client -q "sort_by_label(cyc < 44, 'instance')"

echo "-- sort_by_label_desc(topk(3, lz), 'instance'): the sort starts from the order of topk(), as in Prometheus 3.5"
promql_client -q "sort_by_label_desc(topk(3, lz), 'instance')"

echo "-- sort_by_label(topk(3, lz), 'instance'): the order of Prometheus 3.5"
promql_client -q "sort_by_label(topk(3, lz), 'instance')"

echo "-- sort_by_label(bottomk(3, lz), 'instance'): the order of Prometheus 3.5"
promql_client -q "sort_by_label(bottomk(3, lz), 'instance')"

echo "-- sort_by_label_desc(bottomk(3, lz), 'instance'): the order of Prometheus 3.5"
promql_client -q "sort_by_label_desc(bottomk(3, lz), 'instance')"

echo "-- sort_by_label(up, ''): an empty label name is accepted and sorts by all labels, as in Prometheus"
promql_client -q "sort_by_label(up, '')"

echo "-- sort_by_label_desc(up, ''): reverse of the above"
promql_client -q "sort_by_label_desc(up, '')"

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

echo "-- timeSeriesSortGroupsByTags()"
$CLICKHOUSE_CLIENT -q "
SELECT arrayMap(g -> timeSeriesExtractTag(g, 'host'),
    timeSeriesSortGroupsByTags(
        [timeSeriesTagsToGroup([('host', 'host10')]), timeSeriesTagsToGroup([('host', 'host9')]), timeSeriesTagsToGroup([('host', 'host1')])],
        ['host'], false)) AS hosts"

echo "-- error: sort_by_label requires at least 1 argument"
promql_client -q "sort_by_label()" 2>&1 | grep -o "expects at least 1 argument" | head -n 1

echo "-- error: label arguments must be strings"
promql_client -q "sort_by_label(up, 5)" 2>&1 | grep -o "of type STRING" | head -n 1

echo "-- error: sort_by_label's first argument must be an instant vector"
promql_client -q "sort_by_label(1, 'x')" 2>&1 | grep -o "expects the first argument of type" | head -n 1

echo "-- error: sort takes exactly 1 argument"
promql_client -q "sort()" 2>&1 | grep -o "expects 1 argument" | head -n 1

echo "-- error: sort's argument must be an instant vector"
promql_client -q "sort(1)" 2>&1 | grep -o "expects an argument of type" | head -n 1

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -q "DROP TABLE ts"
