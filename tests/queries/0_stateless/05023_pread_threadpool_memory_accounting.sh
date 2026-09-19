#!/usr/bin/env bash
# Tags: no-object-storage
# no-object-storage: this checks local pread_threadpool attach/detach accounting, not remote reads.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Packed parts are read from a single archive and skip the per-file local reader.
# Many small parts give many threadpool tasks. A large SQL comment is copied into
# ThreadGroup::SharedData::query_for_logs on every attach; if that copy is freed
# after the worker is reparented to total_memory_tracker, query memory grows with
# the task count.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS 05023_pread_threadpool_memory;
    CREATE TABLE 05023_pread_threadpool_memory (id UInt64, s String)
    ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_full_part_storage = 0;
    SYSTEM STOP MERGES 05023_pread_threadpool_memory;
"

for _ in $(seq 1 20); do
    $CLICKHOUSE_CLIENT --query "INSERT INTO 05023_pread_threadpool_memory SELECT number, randomPrintableASCII(64) FROM numbers(200)"
done

# ~68 KiB comment: same size class as the reported worker allocations.
COMMENT=$(printf '%*s' 70000 '' | tr ' ' 'x')

run_scan()
{
    local method=$1
    $CLICKHOUSE_CLIENT --query "
        SELECT count(), max(s) FROM 05023_pread_threadpool_memory /* ${COMMENT} */
        SETTINGS
            local_filesystem_read_method = '${method}',
            local_filesystem_read_prefetch = 0,
            max_untracked_memory = 0,
            max_threads = 2,
            use_uncompressed_cache = 0,
            log_comment = '05023_${method}',
            log_queries = 1
        FORMAT Null
    "
}

run_scan pread
run_scan pread_threadpool

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT --query "
    SELECT
        if(pread_ok AND threadpool_ok AND threadpool_memory <= greatest(pread_memory * 8, toUInt64(64 * 1024 * 1024)),
           'OK',
           concat('FAIL pread=', toString(pread_memory),
                  ' threadpool=', toString(threadpool_memory),
                  ' pread_ok=', toString(pread_ok),
                  ' threadpool_ok=', toString(threadpool_ok)))
    FROM
    (
        SELECT
            maxIf(type = 'QueryFinish', log_comment = '05023_pread') AS pread_ok,
            maxIf(type = 'QueryFinish', log_comment = '05023_pread_threadpool') AS threadpool_ok,
            maxIf(memory_usage, log_comment = '05023_pread' AND type = 'QueryFinish') AS pread_memory,
            maxIf(memory_usage, log_comment = '05023_pread_threadpool' AND type = 'QueryFinish') AS threadpool_memory
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND event_date >= yesterday()
          AND event_time >= now() - 600
          AND log_comment IN ('05023_pread', '05023_pread_threadpool')
    )
"

$CLICKHOUSE_CLIENT --query "DROP TABLE 05023_pread_threadpool_memory"
