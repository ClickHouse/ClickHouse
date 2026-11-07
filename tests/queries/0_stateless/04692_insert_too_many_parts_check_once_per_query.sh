#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `Too many parts` check is only allowed to reject an INSERT before it has written anything.
# With `max_insert_threads` a plain INSERT writes through several sinks running in parallel, and a
# sink that starts after another one has already written the part must not count that part.

# Only synchronous inserts write through parallel sinks, so the parallel settings are pinned together
# with `async_insert = 0` - a CI configuration may enable asynchronous inserts for the whole server.
PARALLEL_INSERT="--async_insert 0 --max_threads 64 --max_insert_threads 64"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE crossing (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS parts_to_throw_insert = 2;
    SYSTEM STOP MERGES crossing;
"

# Every iteration leaves the table with a single part, one below the threshold, and then inserts the
# part that reaches the threshold. That INSERT has to be accepted: the table was below the threshold
# when it started.
for _ in {1..10}
do
    $CLICKHOUSE_CLIENT --query "TRUNCATE TABLE crossing"
    $CLICKHOUSE_CLIENT --async_insert 0 --query "INSERT INTO crossing VALUES (0)"
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $PARALLEL_INSERT --query "INSERT INTO crossing VALUES (1)"
done

$CLICKHOUSE_CLIENT --query "SELECT count() FROM crossing"

# Only the rejection is shared across the streams. The `parts_to_delay_insert` backpressure applies
# per block, so every sink that receives data still delays before writing - the shared pre-write check
# does not weaken the throttling of parallel inserts. `parts_to_delay_insert = 1` makes every block
# written into a non-empty table delay, and `DelayedInserts` counts the delays. The single row of the
# INSERT arrives as one block into one of the sinks, so the query delays exactly once, no matter how
# many streams it writes through (before the fix every stream performed the check on start: 64).
$CLICKHOUSE_CLIENT --async_insert 0 --query "
    CREATE TABLE delayed (x UInt64) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS parts_to_delay_insert = 1, parts_to_throw_insert = 1000, min_delay_to_insert_ms = 0;
    SYSTEM STOP MERGES delayed;
    INSERT INTO delayed VALUES (0);
"

query_id="04692_$CLICKHOUSE_DATABASE"
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT --query_id "$query_id" $PARALLEL_INSERT --query "INSERT INTO delayed VALUES (1)"

$CLICKHOUSE_CLIENT --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT ProfileEvents['DelayedInserts'] FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish';
"

# The gate is shared per destination table, not per view: the branches of different materialized
# views converging on the same target table also perform the rejection check exactly once for the
# whole query. Each of the two branches delivers one block into the target table and delays on it,
# so `DelayedInserts` is 2 (before the fix every stream of every branch performed the check: 128).
$CLICKHOUSE_CLIENT --async_insert 0 --query "
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY tuple();
    CREATE TABLE converged (x UInt64) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS parts_to_delay_insert = 1, parts_to_throw_insert = 1000, min_delay_to_insert_ms = 0;
    CREATE MATERIALIZED VIEW mv_1 TO converged AS SELECT x FROM src;
    CREATE MATERIALIZED VIEW mv_2 TO converged AS SELECT x FROM src;
    SYSTEM STOP MERGES converged;
    INSERT INTO converged VALUES (0);
"

query_id="04692_mv_$CLICKHOUSE_DATABASE"
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT --query_id "$query_id" $PARALLEL_INSERT --parallel_view_processing 1 --query "INSERT INTO src VALUES (1)"

$CLICKHOUSE_CLIENT --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT ProfileEvents['DelayedInserts'] FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish';
"

# An Alias destination forwards the write through a nested INSERT per parallel branch, and the real
# check runs inside those nested inserts. They share the outer query's gate, so the rejection check
# still runs exactly once for the whole query, and the single row delays once in the branch that
# received it (before the fix every branch performed the check on start: 64).
$CLICKHOUSE_CLIENT --async_insert 0 --allow_experimental_alias_table_engine 1 --query "
    CREATE TABLE alias_target (x UInt64) ENGINE = MergeTree ORDER BY tuple()
        SETTINGS parts_to_delay_insert = 1, parts_to_throw_insert = 1000, min_delay_to_insert_ms = 0;
    CREATE TABLE alias_front ENGINE = Alias('alias_target');
    SYSTEM STOP MERGES alias_target;
    INSERT INTO alias_target VALUES (0);
"

query_id="04692_alias_$CLICKHOUSE_DATABASE"
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT --query_id "$query_id" $PARALLEL_INSERT --query "INSERT INTO alias_front VALUES (1)"

$CLICKHOUSE_CLIENT --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT ProfileEvents['DelayedInserts'] FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '$query_id' AND type = 'QueryFinish';
"
