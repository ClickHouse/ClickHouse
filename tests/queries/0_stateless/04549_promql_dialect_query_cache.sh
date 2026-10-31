#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the PromQL grammar requires ANTLR4 which is disabled in the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --allow_experimental_time_series_table 1 -m -q "
CREATE TABLE ts (samples Array(Tuple(DateTime64(3, 'UTC'), Float64))) ENGINE = TimeSeries;
INSERT INTO ts (metric_name, tags, samples) VALUES ('up', {'instance':'host1'}, [(toDateTime64(1700000000, 3, 'UTC'), 1)]);
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
