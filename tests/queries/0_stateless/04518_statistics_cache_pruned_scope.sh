#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas

# The table-wide statistics estimator cache (use_statistics_cache = 1, warmed by the
# background refreshStatistics() task) must serve only queries reading the full part
# set; a partition-pruned query must bypass it and compose statistics over surviving
# parts (issue #110281). Warm-cache detection follows 03707_statistics_cache:
# LoadedStatisticsMicroseconds = 0 <=> the query did not load statistics itself.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

settings="--use_statistics=1 --use_statistics_cache=1 --collect_hash_table_stats_during_joins=0"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS fact_04518 SYNC;
    DROP TABLE IF EXISTS dim_04518 SYNC;
    CREATE TABLE fact_04518 (p UInt8, id UInt64)
    ENGINE = MergeTree PARTITION BY p ORDER BY id
    SETTINGS refresh_statistics_interval = 1;
    CREATE TABLE dim_04518 (id UInt64)
    ENGINE = MergeTree ORDER BY id
    SETTINGS refresh_statistics_interval = 1;
"
$CLICKHOUSE_CLIENT --materialize_statistics_on_insert=1 -q "INSERT INTO fact_04518 SELECT 1, number FROM numbers(100000)"
$CLICKHOUSE_CLIENT --materialize_statistics_on_insert=1 -q "INSERT INTO fact_04518 SELECT 2, number % 10 FROM numbers(1000)"
$CLICKHOUSE_CLIENT --materialize_statistics_on_insert=1 -q "INSERT INTO dim_04518 SELECT number FROM numbers(10000)"

unpruned_query="SELECT count() FROM fact_04518 AS f INNER JOIN dim_04518 AS d ON f.id = d.id FORMAT Null"
pruned_query="SELECT count() FROM fact_04518 AS f INNER JOIN dim_04518 AS d ON f.id = d.id WHERE f.p = 2 FORMAT Null"

loaded_for_comment() {
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT -q "
        SELECT ProfileEvents['LoadedStatisticsMicroseconds']
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
            AND type = 'QueryFinish' AND current_database = currentDatabase()
            AND log_comment = '$1'
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

# 1. Wait for the background refresh to warm the table-wide cache: retry until an
#    unpruned query is served without loading statistics itself.
warm=0
for i in $(seq 1 120); do
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $settings --log_comment "04518-warm-$i" -q "$unpruned_query"
    if [[ "$(loaded_for_comment "04518-warm-$i")" == "0" ]]; then
        warm=1
        break
    fi
    sleep 0.5
done
echo "unpruned warm cache hit: $warm"

# 2. A pruned query must bypass the table-wide cache (loads per-part statistics)...
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $settings --log_comment "04518-pruned" -q "$pruned_query"
loaded=$(loaded_for_comment "04518-pruned")
echo "pruned query bypasses cache: $((loaded > 0))"

# 3. ...and still gets pruned-scope estimates (not the table-wide 101000/2 model).
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $settings -q "
    SELECT trimLeft(explain)
    FROM (EXPLAIN SELECT count() FROM fact_04518 AS f INNER JOIN dim_04518 AS d ON f.id = d.id WHERE f.p = 2)
    WHERE explain LIKE '%⋈%'"

# 4. The unpruned cache hit must be preserved after the pruned query.
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $settings --log_comment "04518-warm-after" -q "$unpruned_query"
echo "unpruned cache hit preserved: $([ "$(loaded_for_comment "04518-warm-after")" == "0" ] && echo 1 || echo 0)"

$CLICKHOUSE_CLIENT -q "DROP TABLE fact_04518 SYNC; DROP TABLE dim_04518 SYNC"
