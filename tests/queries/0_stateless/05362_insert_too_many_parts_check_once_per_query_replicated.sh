#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `ReplicatedMergeTree` sibling of `04692_insert_too_many_parts_check_once_per_query`: with
# `max_insert_threads` a plain INSERT writes through several `ReplicatedMergeTreeSink`s running in
# parallel, and a sink that starts after another one has already committed the part must not count
# that part and reject the very INSERT that wrote it.

# Only synchronous inserts write through parallel sinks, and an INSERT with deduplication is written
# through a single stream, so the parallel settings are pinned together with `async_insert = 0` and
# `insert_deduplicate = 0`. No quorum: a non-parallel quorum insert is written through a single stream.
PARALLEL_INSERT="--async_insert 0 --insert_deduplicate 0 --insert_quorum 0 --max_threads 64 --max_insert_threads 64"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE crossing (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/crossing', 'r1') ORDER BY tuple()
        SETTINGS parts_to_throw_insert = 2;
    SYSTEM STOP MERGES crossing;
"

# Every iteration leaves the table with a single part, one below the threshold, and then inserts the
# part that reaches the threshold. That INSERT has to be accepted: the table was below the threshold
# when it started.
for _ in {1..10}
do
    $CLICKHOUSE_CLIENT --query "TRUNCATE TABLE crossing SETTINGS alter_sync = 2"
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $PARALLEL_INSERT --max_insert_threads 1 --query "INSERT INTO crossing VALUES (0)"
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $PARALLEL_INSERT --query "INSERT INTO crossing VALUES (1)"
done

$CLICKHOUSE_CLIENT --query "SELECT count() FROM crossing"

# Only the rejection is shared across the streams. The `parts_to_delay_insert` backpressure applies
# per block in `ReplicatedMergeTreeSink::consume`, so the single block of the INSERT delays exactly
# once, no matter how many streams it writes through.
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $PARALLEL_INSERT --max_insert_threads 1 --query "
    CREATE TABLE delayed (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/delayed', 'r1') ORDER BY tuple()
        SETTINGS parts_to_delay_insert = 1, parts_to_throw_insert = 1000, min_delay_to_insert_ms = 0;
    SYSTEM STOP MERGES delayed;
    INSERT INTO delayed VALUES (0);
"

query_id="05362_$CLICKHOUSE_DATABASE"
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT --query_id "$query_id" $PARALLEL_INSERT --query "INSERT INTO delayed VALUES (1)"

$CLICKHOUSE_CLIENT --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT ProfileEvents['DelayedInserts'] FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish';
"
