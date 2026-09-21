#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database, no-fasttest
# no-parallel: enables server-global failpoints that delay every insert commit and every
#              deduplication-conflict resolution.
# no-shared-merge-tree: StorageSharedMergeTree has its own sink.
# no-replicated-database: the table is created with an explicit replica name.
# no-fasttest: races an insert against a concurrent insert and DROP PARTITION.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A concurrent insert can re-create the deduplication node that a conflict resolution has just found
# gone, so the resolution can repeat. Every repeat has to reach the insert's retry controller, which
# counts it against insert_keeper_max_retries: a workload that keeps re-creating the node has to end the
# insert with an error rather than keep it running.

# Matches sleepForSeconds() at both failpoints.
DELAY_MS=5000
COMMIT_FAILPOINT=rmt_delay_commit_part
RESOLVE_FAILPOINT=rmt_delay_dedup_conflict_resolution
ZK="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t"
# insert_keeper_max_retries = 0 makes the first counted repeat the last one.
INSERT_SETTINGS="async_insert = 0, insert_deduplicate = 1, insert_keeper_fault_injection_probability = 0, insert_keeper_max_retries = 0"

# The lock request logs this when it refuses the lock, which is immediately before the first resolution.
lock_conflicts()
{
    $CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS text_log;
        SELECT count() FROM system.text_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '$1'
          AND message_format_string = 'Cannot get lock, the conflict path is {}'
        SETTINGS max_rows_to_read = 0"
}

# Keeper rejected the commit transaction because the node appeared behind this insert's back, which is
# the second conflict. The entry is written at the rejection site, so it also dates the resolution that
# the failpoint then holds.
commit_rejections()
{
    $CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS text_log;
        SELECT count() FROM system.text_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '$1'
          AND message_format_string = 'Block with ID {} already exists (it was just appeared) for part {}. Ignore it.'
        SETTINGS max_rows_to_read = 0"
}

last_rejection_at()
{
    $CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS text_log;
        SELECT max(toUnixTimestamp64Micro(event_time_microseconds)) FROM system.text_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '$1'
          AND message_format_string = 'Block with ID {} already exists (it was just appeared) for part {}. Ignore it.'
        SETTINGS max_rows_to_read = 0"
}

locks_held()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$ZK/block_numbers/all'"
}

# Every window below is opened by one of the two failpoints, so a state that has not been reached in
# twice DELAY_MS will not be reached at all.
await()
{
    for _ in {1..50}; do
        [ "$("$1" "$3")" -eq "$2" ] && return 0
        sleep 0.2
    done
    return 1
}

# The insert has to resolve a vanished node twice: once reported by the lock request, once by the commit
# transaction. An attempt that misses any of the four windows proves nothing rather than failing, so it
# is retried on a fresh table.
for attempt in {1..3}; do
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS t_04955 SYNC;
        CREATE TABLE t_04955 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t', 'r1') ORDER BY k;
        SYSTEM ENABLE FAILPOINT $RESOLVE_FAILPOINT;
        INSERT INTO t_04955 SETTINGS $INSERT_SETTINGS VALUES (1);
        SYSTEM ENABLE FAILPOINT $COMMIT_FAILPOINT;"

    # The nonce keeps this unique across executions sharing a database, as every probe here requires.
    query_id="04955_${CLICKHOUSE_DATABASE}_${attempt}_$(random_str 10)"
    victim_err="${CLICKHOUSE_TMP}/04955_$query_id.err"
    $CLICKHOUSE_CLIENT --query_id "$query_id" -q "INSERT INTO t_04955 SETTINGS $INSERT_SETTINGS VALUES (1)" 2> "$victim_err" &
    insert_job=$!

    gone=0
    probed_at=
    rejected_at=
    # First conflict: the lock request refuses, then the node has to disappear while the resolution is
    # held. This insert holds no lock of its own here, so the drop cannot wait on one.
    if await lock_conflicts 1 "$query_id"; then
        $CLICKHOUSE_CLIENT -q "ALTER TABLE t_04955 DROP PARTITION tuple()"

        # The resolution finds the node gone and goes back to allocating a block number. Only once it
        # holds that block number can the other insert create the node behind its back.
        if await locks_held 1; then
            $CLICKHOUSE_CLIENT -q "
                SYSTEM DISABLE FAILPOINT $COMMIT_FAILPOINT;
                INSERT INTO t_04955 SETTINGS $INSERT_SETTINGS VALUES (1);"

            # Second conflict: Keeper rejects the commit. The block number is released before the second
            # resolution starts, and DROP PARTITION warns on stderr while that lock is still held, so the
            # drop waits for it to disappear instead of racing it.
            if await commit_rejections 1 "$query_id" && await locks_held 0; then
                rejected_at=$(last_rejection_at "$query_id")
                $CLICKHOUSE_CLIENT -q "ALTER TABLE t_04955 DROP PARTITION tuple()"
                read -r gone probed_at <<< "$($CLICKHOUSE_CLIENT -q "
                    SELECT count() = 0, toUnixTimestamp64Micro(now64(6))
                    FROM system.zookeeper WHERE path = '$ZK/deduplication_hashes'")"
            fi
        fi
    fi

    wait $insert_job
    # Both failpoints delay every insert on the server, so an attempt that missed a window has to clear
    # them too.
    $CLICKHOUSE_CLIENT -q "
        SYSTEM DISABLE FAILPOINT $RESOLVE_FAILPOINT;
        SYSTEM DISABLE FAILPOINT $COMMIT_FAILPOINT;"

    # Both stamps come from the server clock: the rejection proves the node was still there, and the
    # removal is confirmed less than DELAY_MS later, which is while the second resolution is still held.
    conflicts=$(lock_conflicts "$query_id")
    rejections=$(commit_rejections "$query_id")
    in_window=0
    if [ "$gone" = "1" ] && [ -n "$probed_at" ] && [ -n "$rejected_at" ]; then
        removed_after_us=$((probed_at - rejected_at))
        [ "$removed_after_us" -gt 0 ] && [ "$removed_after_us" -lt "$((DELAY_MS * 1000))" ] && in_window=1
    fi
    bounded=0
    grep -q "UNFINISHED" "$victim_err" && bounded=1
    rm -f "$victim_err"

    [ "$conflicts" = "1" ] && [ "$rejections" = "1" ] && [ "$in_window" = "1" ] && break
done

echo "conflicts reported by the lock request and by the commit $conflicts $rejections"
echo "node removed while resolving the second one $in_window"
echo "insert bounded by the retry limit $bounded"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04955 SYNC"
