#!/usr/bin/env bash
# Tags: no-parallel, no-random-merge-tree-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
$CLICKHOUSE_CLIENT --query "

    DROP TABLE IF EXISTS t_prewarm_cache_rmt_1;

    CREATE TABLE t_prewarm_cache_rmt_1 (a UInt64, b UInt64, c UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03277_prewarms_caches/t_prewarm_cache', '1')
    ORDER BY a
    SETTINGS
        index_granularity = 100,
        min_bytes_for_wide_part = 0,
        use_primary_key_cache = 1,
        prewarm_primary_key_cache = 1,
        prewarm_mark_cache = 1,
        max_cleanup_delay_period = 1,
        cleanup_delay_period = 1,
        min_bytes_to_prewarm_caches = 30000;

    SYSTEM DROP MARK CACHE;
    SYSTEM DROP PRIMARY INDEX CACHE;

    INSERT INTO t_prewarm_cache_rmt_1 SELECT number, rand(), rand() FROM numbers(100, 100);
    INSERT INTO t_prewarm_cache_rmt_1 SELECT number, rand(), rand() FROM numbers(1000, 2000);

    SYSTEM RELOAD ASYNCHRONOUS METRICS;
    SELECT metric, value FROM system.asynchronous_metrics WHERE metric IN ('PrimaryIndexCacheFiles', 'MarkCacheFiles') ORDER BY metric;

    SELECT count() FROM t_prewarm_cache_rmt_1 WHERE a % 2 = 0 AND a >= 100 AND a < 2000 AND NOT ignore(a, b);

    SYSTEM RELOAD ASYNCHRONOUS METRICS;
    SELECT metric, value FROM system.asynchronous_metrics WHERE metric IN ('PrimaryIndexCacheFiles', 'MarkCacheFiles') ORDER BY metric;

    SYSTEM DROP MARK CACHE;
    SYSTEM DROP PRIMARY INDEX CACHE;

    OPTIMIZE TABLE t_prewarm_cache_rmt_1 FINAL;

    SELECT count() FROM t_prewarm_cache_rmt_1 WHERE a % 2 = 0 AND a >= 100 AND a < 2000 AND NOT ignore(a, b);

    SYSTEM RELOAD ASYNCHRONOUS METRICS;
    SELECT metric, value FROM system.asynchronous_metrics WHERE metric IN ('PrimaryIndexCacheFiles', 'MarkCacheFiles') ORDER BY metric;

    TRUNCATE TABLE t_prewarm_cache_rmt_1;
"

for _ in {1..100}; do
    res=$($CLICKHOUSE_CLIENT -q "
        SYSTEM RELOAD ASYNCHRONOUS METRICS;
        SELECT value FROM system.asynchronous_metrics WHERE metric = 'PrimaryIndexCacheFiles';
    ")
    if [[ $res -eq 0 ]]; then
        break
    fi
    sleep 0.3
done

$CLICKHOUSE_CLIENT --query "
    SYSTEM RELOAD ASYNCHRONOUS METRICS;
    SELECT metric, value FROM system.asynchronous_metrics WHERE metric IN ('PrimaryIndexCacheFiles', 'MarkCacheFiles') ORDER BY metric;

    SYSTEM FLUSH LOGS query_log;

    SELECT
        ProfileEvents['LoadedMarksFiles'],
        ProfileEvents['LoadedPrimaryIndexFiles']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT count() FROM t_prewarm_cache_rmt_1%'
    ORDER BY event_time_microseconds;

    DROP TABLE IF EXISTS t_prewarm_cache_rmt_1;
"
