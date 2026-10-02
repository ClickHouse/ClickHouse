#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the PromQL grammar requires ANTLR4 which is disabled in the fast-test build.
# no-replicated-database: the experimental TimeSeries table engine does not round-trip through DatabaseReplicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

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
INSERT INTO ts_tags VALUES ('00000000-0000-0000-0000-000000000001', 'up', {'instance':'host1'}, toDateTime64(1699999000, 3, 'UTC'), toDateTime64(1700001000, 3, 'UTC'));
INSERT INTO ts_data VALUES ('00000000-0000-0000-0000-000000000001', toDateTime64(1700000000, 3, 'UTC'), 1);
"

promql_client()
{
    $CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 --dialect promql --promql_table ts --use_query_cache 1 "$@"
}

echo "-- 'auto' evaluation time transpiles to now(), the query cache must reject it by default"
promql_client -q "up" 2>&1 | grep -o "QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS" | head -1

echo "-- explicit evaluation time is deterministic, the second run is served from the cache"
QUERY_ID_1="${CLICKHOUSE_DATABASE}_promql_query_cache_1"
QUERY_ID_2="${CLICKHOUSE_DATABASE}_promql_query_cache_2"
promql_client --promql_evaluation_time 1700000000 --query_id "$QUERY_ID_1" -q "up"
promql_client --promql_evaluation_time 1700000000 --query_id "$QUERY_ID_2" -q "up"

echo "-- a different PromQL query must not collide with the cached one"
promql_client --promql_evaluation_time 1700000000 -q "up * 2"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase() AND query_id IN ('$QUERY_ID_1', '$QUERY_ID_2')
ORDER BY query_id"
