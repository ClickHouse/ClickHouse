#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings, no-random-settings

# Test that finalize_projection_parts_synchronously actually reduces peak memory
# by not accumulating all projection output streams simultaneously.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="
    CREATE TABLE test_proj_sync
    (
        key UInt64,
        a1 String DEFAULT randomPrintableASCII(100),
        a2 String DEFAULT randomPrintableASCII(100),
        a3 String DEFAULT randomPrintableASCII(100),
        a4 String DEFAULT randomPrintableASCII(100),
        a5 String DEFAULT randomPrintableASCII(100),
        a6 String DEFAULT randomPrintableASCII(100),
        a7 String DEFAULT randomPrintableASCII(100),
        a8 String DEFAULT randomPrintableASCII(100),
        a9 String DEFAULT randomPrintableASCII(100),
        a10 String DEFAULT randomPrintableASCII(100),
        v1 UInt64 DEFAULT rand64(),
        v2 UInt64 DEFAULT rand64(),
        v3 UInt64 DEFAULT rand64(),
        v4 UInt64 DEFAULT rand64(),
        v5 UInt64 DEFAULT rand64(),
        PROJECTION p1 (SELECT key, v1, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10 ORDER BY v1),
        PROJECTION p2 (SELECT key, v2, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10 ORDER BY v2),
        PROJECTION p3 (SELECT key, v3, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10 ORDER BY v3),
        PROJECTION p4 (SELECT key, v4, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10 ORDER BY v4),
        PROJECTION p5 (SELECT key, v5, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10 ORDER BY v5)
    )
    ENGINE = MergeTree
    ORDER BY key
"

# INSERT with async mode (default)
uuid_async=$(cat /proc/sys/kernel/random/uuid)
$CLICKHOUSE_CLIENT --query_id="$uuid_async" --query="
    INSERT INTO test_proj_sync (key) SELECT number FROM numbers(50000)
    SETTINGS finalize_projection_parts_synchronously = 0, max_insert_threads = 1
"

# INSERT with sync mode
uuid_sync=$(cat /proc/sys/kernel/random/uuid)
$CLICKHOUSE_CLIENT --query_id="$uuid_sync" --query="
    INSERT INTO test_proj_sync (key) SELECT number + 50000 FROM numbers(50000)
    SETTINGS finalize_projection_parts_synchronously = 1, max_insert_threads = 1
"

# Compare memory usage: sync mode should use 2x less memory
$CLICKHOUSE_CLIENT --query="
    SYSTEM FLUSH LOGS query_log;
    SELECT
        memory_sync * 2 < memory_async AS sync_uses_less_memory
    FROM
    (
        SELECT memory_usage AS memory_async
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND query_id = '$uuid_async'
            AND type = 'QueryFinish'
    ) AS t1,
    (
        SELECT memory_usage AS memory_sync
        FROM system.query_log
        WHERE current_database = currentDatabase()
            AND query_id = '$uuid_sync'
            AND type = 'QueryFinish'
    ) AS t2
"

$CLICKHOUSE_CLIENT --query="DROP TABLE test_proj_sync"
