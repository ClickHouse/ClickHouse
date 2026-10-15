#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

$CLICKHOUSE_CLIENT --multiquery -q "
    CREATE TABLE readonly_statistics_cache (k UInt64, v UInt64)
    ENGINE = MergeTree ORDER BY k
    SETTINGS auto_statistics_types = 'basic, uniq_v2', refresh_statistics_interval = 1;
    INSERT INTO readonly_statistics_cache SELECT number, number % 100 FROM numbers(10000)
    SETTINGS materialize_statistics_on_insert = 1;
    ALTER TABLE readonly_statistics_cache MODIFY SETTING table_readonly = 1;
    DETACH TABLE readonly_statistics_cache;
    ATTACH TABLE readonly_statistics_cache;
"

# A fresh readonly attachment has no cached estimator. Its startup must schedule
# statistics refresh; a query spanning the full part set will then hit that cache.
warm=0
for i in $(seq 1 40); do
    $CLICKHOUSE_CLIENT -q "SELECT sum(v) FROM readonly_statistics_cache WHERE k % 3 = 0 AND v % 2 = 0
        SETTINGS enable_analyzer = 1, use_statistics = 1, use_statistics_cache = 1,
            use_statistics_for_part_pruning = 0, optimize_move_to_prewhere = 1,
            query_plan_optimize_prewhere = 1, use_query_cache = 0,
            log_comment = '05183-readonly-$i' FORMAT Null"
    $CLICKHOUSE_CLIENT -q 'SYSTEM FLUSH LOGS query_log'
    loaded=$($CLICKHOUSE_CLIENT -q "SELECT ProfileEvents['LoadedStatisticsMicroseconds']
        FROM system.query_log WHERE current_database = currentDatabase()
            AND type = 'QueryFinish' AND log_comment = '05183-readonly-$i'
        ORDER BY event_time_microseconds DESC LIMIT 1")
    if [[ "$loaded" == "0" ]]; then
        warm=1
        break
    fi
    sleep 0.1
done
echo "readonly statistics cache hit: $warm"
$CLICKHOUSE_CLIENT -q 'DROP TABLE readonly_statistics_cache SYNC'
