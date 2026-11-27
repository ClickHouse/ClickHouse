#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-parallel, no-parallel-replicas
# A part moved to `detached` by SYSTEM RESTORE REPLICA must take the marks of its projection, and the marks
# of the projection's own skip indices, out of the caches, so that the part attached back under the same name
# does not read them.
# no-parallel: the oracle reads the process-global mark cache, which another test's
# SYSTEM CLEAR MARK CACHE would empty.
# no-parallel-replicas: a projection cannot serve the query when parallel replicas are
# enabled without a local plan or with aggregation in order.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_mc_proj_detached SYNC"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_mc_proj_detached
    (
        k UInt64,
        a UInt64,
        PROJECTION p (SELECT k, a ORDER BY a) WITH SETTINGS (add_minmax_index_for_numeric_columns = 1)
    )
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mc_proj_detached', 'r1') ORDER BY k
    SETTINGS prewarm_mark_cache = 0, min_bytes_for_wide_part = '1G', index_granularity = 8192"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_mc_proj_detached SELECT number, number * 7 % 100003 FROM numbers(100000)"

# Lose the replica metadata, so that SYSTEM RESTORE REPLICA moves the parts to `detached` and attaches them back.
ZK_PATH=$($CLICKHOUSE_CLIENT -q "SELECT zookeeper_path FROM system.replicas WHERE database = currentDatabase() AND table = 't_mc_proj_detached'")
$CLICKHOUSE_CLIENT -q "DETACH TABLE t_mc_proj_detached"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP REPLICA 'r1' FROM ZKPATH '$ZK_PATH'"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t_mc_proj_detached"

for _ in $(seq 1 100); do
    is_readonly=$($CLICKHOUSE_CLIENT -q "SELECT is_readonly FROM system.replicas WHERE database = currentDatabase() AND table = 't_mc_proj_detached'")
    [ "$is_readonly" = "1" ] && break
    sleep 0.1
done

function read_projection()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT count() FROM t_mc_proj_detached WHERE a < 1000 AND k > 50000
        SETTINGS log_comment = '$1', optimize_use_projections = 1, force_optimize_projection = 1,
            use_skip_indexes = 1, use_query_condition_cache = 0
        FORMAT Null"
}

# The first read loads the marks of the projection and of its index into the caches, the second one finds them there.
read_projection 05243_detached_load
read_projection 05243_detached_cached

$CLICKHOUSE_CLIENT -q "SYSTEM RESTORE REPLICA t_mc_proj_detached"
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA t_mc_proj_detached"

read_projection 05243_detached_restored

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# The read after the restore has to load all the marks again, like the first read did.
$CLICKHOUSE_CLIENT -q "
    SELECT
        sumIf(ProfileEvents['MarkCacheHits'], log_comment = '05243_detached_cached') > 0
            AND sumIf(ProfileEvents['MarkCacheMisses'], log_comment = '05243_detached_cached') = 0 AS marks_found_in_cache,
        sumIf(ProfileEvents['MarkCacheMisses'], log_comment = '05243_detached_load') > 0
            AND sumIf(ProfileEvents['MarkCacheMisses'], log_comment = '05243_detached_restored')
                = sumIf(ProfileEvents['MarkCacheMisses'], log_comment = '05243_detached_load') AS marks_loaded_again
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
        AND log_comment LIKE '05243_detached_%' AND type = 'QueryFinish'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_mc_proj_detached SYNC"
