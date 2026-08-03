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

# The check itself has to be performed exactly once per INSERT query, no matter how many streams the
# query writes through. `parts_to_delay_insert = 1` makes every INSERT into a non-empty table delay,
# and `DelayedInserts` counts how many times the check was performed.
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
