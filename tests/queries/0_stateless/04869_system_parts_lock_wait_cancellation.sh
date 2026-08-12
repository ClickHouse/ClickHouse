#!/usr/bin/env bash
#
# Tests that a `system.parts` query honors its time limit even while it is waiting for the table
# share lock in `tryLockTable`: the lock is acquired in short slices with a cancellation check
# between the attempts, instead of a single uninterruptible wait for the whole
# `lock_acquire_timeout`.
#
# The setup makes the share-lock request block: a long-running `SELECT` holds a share lock on the
# table, and a `DROP TABLE` queues for the exclusive lock behind it, so the subsequent share-lock
# request of the `system.parts` query has to wait behind the pending exclusive request. The table
# lives in an `Ordinary` database, because for `Atomic` databases `DROP TABLE` is deferred and
# does not take the exclusive table lock in the foreground. A query with a 1 second deadline in
# the 'break' overflow mode must then give up on the lock and return well before the reader
# releases the lock (about 15 seconds), which it can only do by polling the query status between
# the lock attempts.
#
# Only the upper bound is asserted, so concurrently running instances of this test do not affect
# each other.

# The use of an Ordinary database emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ORDINARY_DB="${CLICKHOUSE_DATABASE}_04869_ordinary"

$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary=1 --query "
DROP DATABASE IF EXISTS $ORDINARY_DB;
CREATE DATABASE $ORDINARY_DB ENGINE = Ordinary;
CREATE TABLE $ORDINARY_DB.t_parts_lock_wait (x UInt64) ENGINE = MergeTree ORDER BY x PARTITION BY x;
INSERT INTO $ORDINARY_DB.t_parts_lock_wait SELECT number FROM numbers(5);
"

READER_QUERY_ID="${CLICKHOUSE_DATABASE}_04869_reader_$$"
DROP_QUERY_ID="${CLICKHOUSE_DATABASE}_04869_drop_$$"

# Holds a share lock on the table for about 15 seconds (killed earlier at the end of the test).
# The table has 5 single-row parts, so the rows arrive in single-row blocks and each block sleeps
# only 3 seconds: this keeps every block under the per-block sleep limit and lets KILL QUERY take
# effect between the blocks. Parallel replicas are disabled explicitly: the test relies on the
# query holding the table share lock locally for its whole duration, and the randomized settings
# may turn parallel replicas on.
$CLICKHOUSE_CLIENT --query_id "$READER_QUERY_ID" --query "
SELECT sleepEachRow(3) FROM $ORDINARY_DB.t_parts_lock_wait
SETTINGS max_block_size = 1, max_threads = 1, enable_parallel_replicas = 0,
         function_sleep_max_microseconds_per_block = 10000000 FORMAT Null;
" 2>/dev/null &
reader_pid=$!

function wait_for_query()
{
    for _ in {1..100}
    do
        result=$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$1'")
        [ "$result" == "1" ] && return
        sleep 0.1
    done
    echo "query $1 did not start in time"
}

wait_for_query "$READER_QUERY_ID"

# Queues for the exclusive drop lock behind the reader; subsequent share-lock requests wait
# behind this pending exclusive request.
$CLICKHOUSE_CLIENT --query_id "$DROP_QUERY_ID" --query "DROP TABLE $ORDINARY_DB.t_parts_lock_wait" &
drop_pid=$!

wait_for_query "$DROP_QUERY_ID"
# The query registers in the processlist before it requests the lock; give it a moment to actually
# block on the lock request.
sleep 1

# The deadline expires while the query waits for the share lock of the table, and in the 'break'
# mode it must return the rows collected so far instead of waiting out the reader or the
# lock_acquire_timeout.
start=$(date +%s)
$CLICKHOUSE_CLIENT --query "
SELECT name FROM system.parts WHERE database = '$ORDINARY_DB' AND table = 't_parts_lock_wait'
FORMAT Null
SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break', lock_acquire_timeout = 60;
"
end=$(date +%s)
echo "break query returned early $((end - start < 6))"

# Let the DROP TABLE proceed without waiting out the whole reader sleep.
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$READER_QUERY_ID' SYNC FORMAT Null"

wait $reader_pid $drop_pid 2>/dev/null || true

$CLICKHOUSE_CLIENT --query "DROP DATABASE $ORDINARY_DB"
