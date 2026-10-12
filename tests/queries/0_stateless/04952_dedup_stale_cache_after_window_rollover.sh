#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-replicated-database, no-fasttest
# no-shared-merge-tree: StorageSharedMergeTree has its own sink.
# no-replicated-database: the table is created with an explicit replica name.
# no-fasttest: waits for the background cleanup thread to prune the deduplication window.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# An expiring deduplication window is a second way for a hash to leave Keeper while the in-memory
# prefilter cache still holds it, with no DDL involved at all (#121174). Keeper is the authority, so
# an async insert whose hash is gone from it must land.

HASHES="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t/deduplication_hashes"
INSERT_SETTINGS="async_insert = 1, wait_for_async_insert = 1, async_insert_deduplicate = 1,
    async_insert_busy_timeout_min_ms = 10, async_insert_busy_timeout_max_ms = 20,
    insert_keeper_fault_injection_probability = 0"

insert_one()
{
    $CLICKHOUSE_CLIENT -q "INSERT INTO t_04952 SETTINGS $INSERT_SETTINGS VALUES ($1)"
}

# Whether the cache prefiltered any insert of this incarnation of the table, restricted with $1 to
# the inserts that were deduplicated or to the one that committed.
cache_hit()
{
    $CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS part_log;
        SELECT countIf(ProfileEvents['AsyncInsertCacheHits'] > 0) > 0
        FROM system.part_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND database = currentDatabase() AND table = 't_04952'
          AND table_uuid = (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_04952')
          AND event_type = 'NewPart' AND $1"
}

# The cache is a snapshot refreshed only when a conflict is detected, so the last insert has to run
# after the prune and before any such refresh. A refresh winning that race proves nothing rather
# than failing, so start over; a dropped row is reported at once.
for _ in {1..5}; do
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS t_04952 SYNC;
        CREATE TABLE t_04952 (k UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t', 'r1') ORDER BY k
        SETTINGS replicated_deduplication_window = 2, cleanup_delay_period = 1,
            cleanup_delay_period_random_add = 0, cleanup_thread_preferred_points_per_iteration = 0;"

    # The only hash in the window, so nothing can be pruned while the cache is armed below.
    insert_one 1
    target=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.zookeeper WHERE path = '$HASHES'")

    # Keeper catches the first duplicate and that triggers the cache update; the cache is armed once
    # a duplicate is caught by the cache itself.
    for _ in {1..10}; do
        insert_one 1
        [ "$(cache_hit 'error != 0')" = "1" ] && break
    done

    # Roll the window past the armed hash.
    insert_one 2
    insert_one 3

    pruned=0
    for _ in {1..120}; do
        if [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$HASHES' AND name = '$target'")" = "0" ]; then
            pruned=1
            break
        fi
        sleep 0.5
    done

    insert_one 1
    rows=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM t_04952")
    hit=$(cache_hit 'error = 0')

    [ "$hit" = "1" ] && break
    [ "$rows" != "4" ] && break
done

echo "hash pruned from keeper $pruned"
echo "rows after reinsert $rows"
echo "cache hit observed $hit"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04952 SYNC"
