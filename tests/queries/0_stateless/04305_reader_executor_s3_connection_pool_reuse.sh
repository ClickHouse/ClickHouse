#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-replicated-database, no-distributed-cache, no-parallel-replicas
#
# no-fasttest -- needs an S3-backed disk
# no-distributed-cache -- the executor falls back to legacy with distributed cache, so the
#                         connection-pool path under test is not exercised

# When the ReaderExecutor reads a column object from S3 it borrows an HTTP
# connection from the DiskConnections pool. A live connection is read to the
# object end and then returned to the pool in a reusable state, so a later read
# of the same endpoint reuses it instead of opening a new one. This verifies, for
# a query whose source reads went through the executor (`LiveSourceBufferCreated`),
# that the executor both reuses pooled connections (`DiskConnectionsReused`) and
# returns the connections it uses to the pool reusable rather than abandoning them
# mid-stream (`DiskConnectionsPreserved`). A warm scan fills the pool, and a second
# cold scan (cache dropped, so it reads from the source again) reuses and preserves.
#
# remote_filesystem_read_prefetch=0 + max_threads=1 keep the reads synchronous so
# the connections opened by the warm scan are idle in the pool by the time the
# measured scan asks for them. Pool reuse still races with parallel activity on
# the shared endpoint, so the measurement is retried a few times (as in
# 02789_reading_from_s3_with_connection_pool).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_re_pool"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_re_pool (c1 UInt32, c2 UInt32, c3 UInt32, c4 UInt32, c5 UInt32)
    ENGINE = MergeTree ORDER BY c1
    SETTINGS index_granularity = 512, min_bytes_for_wide_part = 0, storage_policy = 's3_cache'
"
$CLICKHOUSE_CLIENT --query "
    INSERT INTO t_re_pool SELECT number, number, number, number, number FROM numbers(512 * 64)
"

# Executor on, prefetch off, single-threaded so connection use is sequential.
RE_SETTINGS=(--use_reader_executor=1 --remote_filesystem_read_prefetch=0 --max_threads=1)
SCAN="SELECT count() FROM t_re_pool WHERE NOT ignore(c1, c2, c3, c4, c5) FORMAT Null"

for _ in {0..19}
do
    # Warm the pool: a cold scan from the source opens connections and returns
    # them to the pool when the columns are fully read.
    $CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE"
    $CLICKHOUSE_CLIENT "${RE_SETTINGS[@]}" --query "$SCAN"

    # Measured: read from the source again (cache dropped); the executor should
    # reuse the connections the warm scan left in the pool.
    MEASURED_ID="04305_re_pool_${CLICKHOUSE_DATABASE}_$RANDOM"
    $CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE"
    $CLICKHOUSE_CLIENT "${RE_SETTINGS[@]}" --query_id "$MEASURED_ID" --query "$SCAN"

    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

    RES=$($CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['LiveSourceBufferCreated'] > 0
           AND ProfileEvents['DiskConnectionsReused'] > 0
           AND ProfileEvents['DiskConnectionsPreserved'] > 0
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
            AND current_database = currentDatabase()
            AND query_id = '$MEASURED_ID'
    ")

    [[ $RES -eq 1 ]] && echo "executor reused and preserved pooled connections" && break
done

$CLICKHOUSE_CLIENT --query "DROP TABLE t_re_pool"
