#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database, no-fasttest
# no-parallel: enables a server-global failpoint that delays every deduplication-conflict resolution.
# no-shared-merge-tree: StorageSharedMergeTree has its own sink.
# no-replicated-database: the table is created with an explicit replica name.
# no-fasttest: races a synchronous insert against a concurrent DROP PARTITION.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A synchronous insert reaches the same conflict resolution as an asynchronous one, but through
# Keeper's own ZNODEEXISTS on the deduplication node instead of the prefilter cache, and the node can
# be dropped while the resolution is in flight. Keeper is the authority, so the insert must land.

# Matches sleepForSeconds() at the rmt_delay_dedup_conflict_resolution failpoint.
DELAY_MS=5000
FAILPOINT=rmt_delay_dedup_conflict_resolution
HASHES="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t/deduplication_hashes"
INSERT_SETTINGS="async_insert = 0, insert_deduplicate = 1, insert_keeper_fault_injection_probability = 0"

# Keeper answered the lock request for this insert with the conflicting node, which is what sends it
# into the resolution the failpoint then holds. Proving it from the insert's own log entry, rather
# than from its duration, excludes the run where the drop finished first and it never conflicted.
conflicted()
{
    $CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS text_log;
        SELECT count() > 0 FROM system.text_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '$1'
          AND message_format_string = 'Cannot get lock, the conflict path is {}'
        SETTINGS max_rows_to_read = 0"
}

# The drop has to remove the node after the insert conflicted on it and before the resolution reads
# it. The first bound is the log entry above; the second holds because the node is confirmed gone
# less than DELAY_MS after the insert started. Only both bounds together tell a dropped row from an
# insert Keeper deduplicated legitimately, so an attempt that misses either is retried, not failed.
for attempt in {1..5}; do
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS t_04953 SYNC;
        CREATE TABLE t_04953 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t', 'r1') ORDER BY k;
        INSERT INTO t_04953 SETTINGS $INSERT_SETTINGS VALUES (1);"

    hash=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.zookeeper WHERE path = '$HASHES'")
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FAILPOINT"

    # The nonce keeps this unique across executions sharing a database, as conflicted() requires.
    query_id="04953_${CLICKHOUSE_DATABASE}_${attempt}_$(random_str 10)"
    started=$(date +%s%3N)
    $CLICKHOUSE_CLIENT --query_id "$query_id" -q "INSERT INTO t_04953 SETTINGS $INSERT_SETTINGS VALUES (1)" &
    insert_job=$!

    sleep 0.5
    $CLICKHOUSE_CLIENT -q "ALTER TABLE t_04953 DROP PARTITION tuple()"
    gone=$($CLICKHOUSE_CLIENT -q "SELECT count() = 0 FROM system.zookeeper WHERE path = '$HASHES' AND name = '$hash'")
    dropped_ms=$(( $(date +%s%3N) - started ))

    wait $insert_job
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FAILPOINT"

    conflict=$(conflicted "$query_id")
    in_window=$(( gone == 1 && dropped_ms < DELAY_MS ? 1 : 0 ))
    rows=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM t_04953")

    [ "$conflict" = "1" ] && [ "$in_window" = "1" ] && break
done

echo "keeper reported the conflict $conflict"
echo "node removed while resolving $in_window"
echo "rows after the race $rows"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04953 SYNC"
