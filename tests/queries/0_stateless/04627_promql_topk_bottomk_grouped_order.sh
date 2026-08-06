#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the PromQL grammar requires ANTLR4 which is disabled in the fast-test build.
# no-replicated-database: the experimental TimeSeries table engine does not round-trip through DatabaseReplicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Grouped topk/bottomk must keep each bucket's rows consecutive and value-sorted within the bucket
# (bucket order is a deterministic content hash); input order in each bucket is opposite to value order.

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -m -q "
CREATE TABLE ts_data (id UUID, timestamp DateTime64(3, 'UTC'), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE ts_tags (
    id UUID,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time SimpleAggregateFunction(min, Nullable(DateTime64(3, 'UTC'))),
    max_time SimpleAggregateFunction(max, Nullable(DateTime64(3, 'UTC'))))
ENGINE = AggregatingMergeTree ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1;
CREATE TABLE ts_metrics (metric_family_name String, type String, unit String, help String) ENGINE = ReplacingMergeTree ORDER BY metric_family_name;
CREATE TABLE ts ENGINE = TimeSeries DATA ts_data TAGS ts_tags METRICS ts_metrics;

INSERT INTO ts_tags VALUES
    ('00000000-0000-0000-0000-000000000001', 'up', {'job':'a', 'instance':'i1'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000002', 'up', {'job':'a', 'instance':'i2'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000003', 'up', {'job':'b', 'instance':'i3'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC')),
    ('00000000-0000-0000-0000-000000000004', 'up', {'job':'b', 'instance':'i4'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC'));

INSERT INTO ts_data VALUES
    ('00000000-0000-0000-0000-000000000001', toDateTime64(1700000000, 3, 'UTC'), 10),
    ('00000000-0000-0000-0000-000000000002', toDateTime64(1700000000, 3, 'UTC'), 30),
    ('00000000-0000-0000-0000-000000000003', toDateTime64(1700000000, 3, 'UTC'), 20),
    ('00000000-0000-0000-0000-000000000004', toDateTime64(1700000000, 3, 'UTC'), 5);
"

promql_client()
{
    $CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --promql_evaluation_time 1700000000 "$@"
}

echo "-- topk by (job): descending within each bucket, buckets consecutive"
promql_client -q "topk by (job) (2, up)"

echo "-- bottomk by (job): ascending within each bucket, buckets consecutive"
promql_client -q "bottomk by (job) (2, up)"

echo "-- topk without (instance): same buckets as by (job)"
promql_client -q "topk without (instance) (2, up)"

echo "-- topk by (job) with k=1: only each bucket's winner survives"
promql_client -q "topk by (job) (1, up)"

echo "-- grouped topk's order propagates through or: sorted left prefix, bucket-ordered right suffix"
promql_client -q 'sort_desc(up{job="a"}) or topk by (job) (2, up{job="b"})'

echo "-- the order is deterministic regardless of max_threads"
query="topk by (job) (2, up)"
diff <(promql_client -q "$query") <(promql_client -q "$query" --max_threads 1) && echo "OK: same row order regardless of max_threads"

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -m -q "
DROP TABLE ts;
DROP TABLE ts_data;
DROP TABLE ts_tags;
DROP TABLE ts_metrics;
"
