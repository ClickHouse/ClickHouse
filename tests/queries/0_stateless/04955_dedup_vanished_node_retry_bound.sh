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
# the second conflict.
commit_rejections()
{
    $CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS text_log;
        SELECT count() FROM system.text_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '$1'
          AND message_format_string = 'Block with ID {} already exists (it was just appeared) for part {}. Ignore it.'
        SETTINGS max_rows_to_read = 0"
}

locks_held()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$ZK/block_numbers/all'"
}

# A PreActive part means the insert is past renameTempPartAndAdd and has nothing left but the commit that
# the failpoint delays.
parts_awaiting_commit()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts
        WHERE database = currentDatabase() AND table = 't_04955' AND _state = 'PreActive'"
}

# Every state below is opened by a 5 s failpoint sleep, so one that has not appeared in 12 s will not
# appear. The deadline is wall clock, so a slow probe cannot stretch it.
await()
{
    local deadline=$(($(date +%s) + 12))
    while :; do
        [ "$("$1" "$3")" = "$2" ] && return 0
        [ "$(date +%s)" -lt "$deadline" ] || return 1
        sleep 0.2
    done
}

# The insert has to resolve a vanished node twice: once reported by the lock request, once by the commit
# transaction. An attempt that misses one of those windows proves nothing rather than failing, so it is
# retried on a fresh table, but not past the point where the run itself would be killed for running long.
attempt_deadline=$(($(date +%s) + 100))
for attempt in {1..4}; do
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

    removed_while_resolving=0
    # First conflict: the lock request refuses, then the node has to disappear while the resolution is
    # held. This insert holds no lock of its own here, so the drop cannot wait on one.
    if await lock_conflicts 1 "$query_id"; then
        $CLICKHOUSE_CLIENT -q "ALTER TABLE t_04955 DROP PARTITION tuple()"

        # The resolution finds the node gone and goes back to allocating a block number. Disabling the
        # failpoint only leaves this insert waiting once it is already waiting at it, and only then can
        # the other one create the node behind its back.
        if await parts_awaiting_commit 1; then
            $CLICKHOUSE_CLIENT -q "
                SYSTEM DISABLE FAILPOINT $COMMIT_FAILPOINT;
                INSERT INTO t_04955 SETTINGS $INSERT_SETTINGS VALUES (1);"

            # Second conflict: Keeper rejects the commit, which releases the block number before the
            # second resolution starts. DROP PARTITION warns on stderr while that lock is still held, so
            # the drop waits for it to disappear instead of racing it.
            if await locks_held 0; then
                # The removal and the check that this insert is still resolving are one round trip
                # because both have to describe the same instant of the resolution.
                removed_while_resolving=$($CLICKHOUSE_CLIENT -q "
                    ALTER TABLE t_04955 DROP PARTITION tuple();
                    SELECT (SELECT count() FROM system.zookeeper WHERE path = '$ZK/deduplication_hashes') = 0
                       AND (SELECT count() FROM system.processes WHERE query_id = '$query_id') = 1")
            fi
        fi
    fi

    wait $insert_job
    # Both failpoints delay every insert on the server, so an attempt that missed a window has to clear
    # them too.
    $CLICKHOUSE_CLIENT -q "
        SYSTEM DISABLE FAILPOINT $RESOLVE_FAILPOINT;
        SYSTEM DISABLE FAILPOINT $COMMIT_FAILPOINT;"

    conflicts=$(lock_conflicts "$query_id")
    rejections=$(commit_rejections "$query_id")
    bounded=0
    grep -q "keep being created and removed" "$victim_err" && bounded=1
    rm -f "$victim_err"

    # Only the premises decide whether the attempt materialized, never the assertion they support.
    [ "$conflicts" = "1" ] && [ "$rejections" = "1" ] && [ "$removed_while_resolving" = "1" ] && break
    [ "$(date +%s)" -lt "$attempt_deadline" ] || break
done

echo "conflicts reported by the lock request and by the commit $conflicts $rejections"
echo "node removed while the insert was still resolving $removed_while_resolving"
echo "insert bounded by the retry limit $bounded"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04955 SYNC"
