#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-object-storage, no-random-settings

# Regression guard: a warm ReaderExecutor read must not churn the server-shared
# SourceBufferLimit slot. On a warm re-scan over an s3_cache disk the executor
# reserves a connection slot only for the source reads it actually issues - never
# one per window. Before the fix, with prefetch on, maybeTriggerPrefetch reached
# ensurePreAcquiredSlot at every file-end / mark-range extent boundary and then
# bailed, reserving + releasing a slot on the process-wide limit per window
# (~95 reservations for a single warm scan that did one tiny source read).
#
# Invariant checked: ReaderExecutorBufferSlotAcquired <= ReaderExecutorSourceRequests
# (every reserved slot backs a real source read), for both prefetch on and off.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_re_warm_slot"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_re_warm_slot (c1 UInt32, c2 UInt32, c3 UInt32, c4 UInt32, c5 UInt32)
    ENGINE = MergeTree ORDER BY c1
    SETTINGS index_granularity = 512, min_bytes_for_wide_part = '10G', storage_policy = 's3_cache'
"
$CLICKHOUSE_CLIENT --query "
    INSERT INTO t_re_warm_slot SELECT number, number, number, number, number FROM numbers(512 * 32 * 40)
"

run_warm () {  # $1=label  $2=prefetch
    local id="04337_${1}_${CLICKHOUSE_DATABASE}"
    # Cold populate (drop cache first), then a warm re-scan, measured.
    $CLICKHOUSE_CLIENT --query "SYSTEM DROP FILESYSTEM CACHE"
    $CLICKHOUSE_CLIENT --use_reader_executor=1 --remote_filesystem_read_prefetch="$2" \
        --query "SELECT count() FROM t_re_warm_slot WHERE NOT ignore(*) FORMAT Null"
    $CLICKHOUSE_CLIENT --use_reader_executor=1 --remote_filesystem_read_prefetch="$2" --query_id "$id" \
        --query "SELECT count() FROM t_re_warm_slot WHERE NOT ignore(*) FORMAT Null"
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    echo "== warm read, prefetch=$2: served_from_cache, no_slot_churn =="
    $CLICKHOUSE_CLIENT --query "
        SELECT
            ProfileEvents['ReaderExecutorBytesFromFilesystemCache'] > 0 AS served_from_cache,
            ProfileEvents['ReaderExecutorBufferSlotAcquired']
                <= ProfileEvents['ReaderExecutorSourceRequests'] AS no_slot_churn
        FROM system.query_log
        WHERE query_id = '$id' AND type = 'QueryFinish' AND current_database = currentDatabase()
    "
}

run_warm prefetch_on  1
run_warm prefetch_off 0

$CLICKHOUSE_CLIENT --query "DROP TABLE t_re_warm_slot"
