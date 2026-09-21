#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database, no-fasttest
# no-parallel: enables server-global failpoints that delay every insert commit and every
#              deduplication-conflict resolution.
# no-shared-merge-tree: StorageSharedMergeTree has its own sink.
# no-replicated-database: the table is created with an explicit replica name.
# no-fasttest: races two synchronous inserts against a concurrent DROP PARTITION.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The deduplication node can also appear after the block number has been allocated, and then Keeper
# reports the duplicate by rejecting the commit transaction instead of by refusing the lock. The insert
# has already renamed its part into the working set at that point, and the resolution which follows has
# to reach the same verdict: Keeper is the authority, so a node that is gone must not suppress rows.

# Matches sleepForSeconds() at the rmt_delay_dedup_conflict_resolution failpoint.
DELAY_MS=5000
COMMIT_FAILPOINT=rmt_delay_commit_part
RESOLVE_FAILPOINT=rmt_delay_dedup_conflict_resolution
ZK="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t"
INSERT_SETTINGS="async_insert = 0, insert_deduplicate = 1, insert_keeper_fault_injection_probability = 0"

# Keeper rejected this insert's commit transaction because the node had appeared behind its back, which
# is the carrier under test and not the lock request the sibling test covers. The entry is written at
# the rejection site, so it also dates the resolution that the failpoint then holds.
commit_rejected_at()
{
    $CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS text_log;
        SELECT toUnixTimestamp64Micro(event_time_microseconds) FROM system.text_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '$1'
          AND message_format_string = 'Block with ID {} already exists (it was just appeared) for part {}. Ignore it.'
        ORDER BY event_time_microseconds LIMIT 1
        SETTINGS max_rows_to_read = 0"
}

locked()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$ZK/block_numbers/all'"
}

# The drop has to remove the node after the commit was rejected and before the resolution reads it.
# Both bounds come from the server clock: the rejection proves the node was still there, and the
# removal is confirmed less than DELAY_MS later. An attempt that misses either proves nothing rather
# than failing, so it is retried on a fresh table; a dropped row is reported at once.
for attempt in {1..5}; do
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS t_04954 SYNC;
        CREATE TABLE t_04954 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t', 'r1') ORDER BY k;
        SYSTEM ENABLE FAILPOINT $COMMIT_FAILPOINT;
        SYSTEM ENABLE FAILPOINT $RESOLVE_FAILPOINT;"

    # The nonce keeps this unique across executions sharing a database, as commit_rejected_at requires.
    query_id="04954_${CLICKHOUSE_DATABASE}_${attempt}_$(random_str 10)"
    $CLICKHOUSE_CLIENT --query_id "$query_id" -q "INSERT INTO t_04954 SETTINGS $INSERT_SETTINGS VALUES (1)" &
    insert_job=$!

    # Only once this insert holds its block number can the other one create the node behind its back.
    for _ in {1..100}; do
        [ "$(locked)" != "0" ] && break
        sleep 0.1
    done

    # The other insert must not be delayed: it has to commit the node inside that window.
    $CLICKHOUSE_CLIENT -q "
        SYSTEM DISABLE FAILPOINT $COMMIT_FAILPOINT;
        INSERT INTO t_04954 SETTINGS $INSERT_SETTINGS VALUES (1);"

    # The block number is unlocked as soon as Keeper has answered the commit transaction, so the lock
    # disappearing is when the resolution begins. DROP PARTITION waits for that lock and warns while it
    # is held, which would fail the test on stderr, so an attempt that never gets there drops nothing
    # and is retried on its premises instead.
    unlocked=0
    for _ in {1..150}; do
        [ "$(locked)" = "0" ] && { unlocked=1; break; }
        sleep 0.2
    done
    [ "$unlocked" = "1" ] && $CLICKHOUSE_CLIENT -q "ALTER TABLE t_04954 DROP PARTITION tuple()"
    read -r gone probed_at <<< "$($CLICKHOUSE_CLIENT -q "
        SELECT count() = 0, toUnixTimestamp64Micro(now64(6))
        FROM system.zookeeper WHERE path = '$ZK/deduplication_hashes'")"

    wait $insert_job
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $RESOLVE_FAILPOINT"

    rejected_at=$(commit_rejected_at "$query_id")
    rejected=0
    [ -n "$rejected_at" ] && rejected=1
    in_window=0
    if [ "$rejected" = "1" ] && [ "$gone" = "1" ] && [ -n "$probed_at" ]; then
        removed_after_us=$((probed_at - rejected_at))
        [ "$removed_after_us" -gt 0 ] && [ "$removed_after_us" -lt "$((DELAY_MS * 1000))" ] && in_window=1
    fi
    rows=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM t_04954")

    [ "$rejected" = "1" ] && [ "$in_window" = "1" ] && break
done

echo "keeper rejected the commit $rejected"
echo "node removed while resolving $in_window"
echo "rows after the race $rows"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04954 SYNC"
