#!/usr/bin/env bash
#
# Tests that a `system.parts` query honors its time limit even while it is waiting for the table
# share lock in `tryLockTable`: the lock is acquired in short slices with a cancellation check
# between the attempts, instead of a single uninterruptible wait for the whole
# `lock_acquire_timeout`, including its zero-value infinite-wait mode.
#
# The setup makes the share-lock request block: a long-running `SELECT` holds a share lock on the
# table, and a `DROP TABLE` queues for the exclusive lock behind it, so the subsequent share-lock
# request of the `system.parts` query has to wait behind the pending exclusive request. The table
# lives in an `Ordinary` database, because an `Atomic` `DROP TABLE` is deferred and does not take
# the exclusive table lock in the foreground. A query with a 1 second deadline in the 'break'
# overflow mode must then give up on the lock and return well before the reader releases the lock
# (about 15 seconds), which it can only do by polling the query status between the lock attempts.
#
# Only the upper bound is asserted, so concurrently running instances of this test do not affect
# each other.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ORDINARY_DB="${CLICKHOUSE_DATABASE}_04869_ordinary"

# Creating the first `Ordinary` database in the lifetime of the server makes it log a deprecation
# warning, which the client of that very session forwards to stderr, and the test runner treats any
# stderr output as a failure. Whether this test is the one that creates that first database depends
# on the order of the tests, so the fixture is created by a client that does not ask for the server
# logs at all.
CLICKHOUSE_CLIENT_NO_SERVER_LOGS=$(echo "$CLICKHOUSE_CLIENT" | sed "s/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL:-none}/--send_logs_level=none/g")

$CLICKHOUSE_CLIENT_NO_SERVER_LOGS --allow_deprecated_database_ordinary=1 --query "
DROP DATABASE IF EXISTS $ORDINARY_DB;
CREATE DATABASE $ORDINARY_DB ENGINE = Ordinary;
CREATE TABLE $ORDINARY_DB.t_parts_lock_wait (x UInt64) ENGINE = MergeTree ORDER BY x PARTITION BY x;
INSERT INTO $ORDINARY_DB.t_parts_lock_wait SELECT number FROM numbers(5);
"

READER_QUERY_ID="${CLICKHOUSE_DATABASE}_04869_reader_$$"
DROP_QUERY_ID="${CLICKHOUSE_DATABASE}_04869_drop_$$"
TEST_RUN_SUFFIX="${CLICKHOUSE_TEST_UNIQUE_NAME}_$$"
QUERY_LOG_COMMENT="04869_break_query_${TEST_RUN_SUFFIX}"
INFINITE_LOCK_QUERY_LOG_COMMENT="04869_infinite_lock_break_query_${TEST_RUN_SUFFIX}"

# Best-effort cleanup that also runs on the failure paths (`wait_for_query` / `wait_for_drop_lock`
# exit early): without it a failure would leak the 15-second reader, the queued `DROP TABLE` and
# the `Ordinary` database into the rest of the test run. Every step is idempotent, so running it
# again after the happy path is harmless.
function cleanup()
{
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id IN ('$READER_QUERY_ID', '$DROP_QUERY_ID') SYNC FORMAT Null" >/dev/null 2>&1 ||:
    wait 2>/dev/null ||:
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS $ORDINARY_DB" >/dev/null 2>&1 ||:
}

trap cleanup EXIT

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
    # The lock contention is the precondition of the whole test, so a query that never started
    # must fail it instead of letting the check below pass on an unblocked lock request.
    echo "query $1 did not start in time"
    exit 1
}

wait_for_query "$READER_QUERY_ID"

# Queues for the exclusive drop lock behind the reader; subsequent share-lock requests wait
# behind this pending exclusive request.
$CLICKHOUSE_CLIENT --query_id "$DROP_QUERY_ID" --query "DROP TABLE $ORDINARY_DB.t_parts_lock_wait" &
drop_pid=$!

wait_for_query "$DROP_QUERY_ID"

function wait_for_drop_lock()
{
    # The DROP query registers in the processlist before it requests the exclusive lock. A share-lock
    # probe is compatible with the reader, but once the DROP request is queued it waits behind that
    # request and reaches its lock timeout. This proves the contention needed by the queries below.
    for _ in {1..10}
    do
        $CLICKHOUSE_CLIENT --query "
            SELECT name FROM system.parts WHERE database = '$ORDINARY_DB' AND table = 't_parts_lock_wait'
            FORMAT Null
            SETTINGS lock_acquire_timeout = 1;
        " >/dev/null 2>&1 && sleep 0.1 || return
    done

    echo "DROP TABLE did not acquire the table-lock queue in time"
    exit 1
}

wait_for_drop_lock

# The deadline expires while the query waits for the share lock of the table, and in the 'break'
# mode it must return the rows collected so far instead of waiting out the reader or the
# lock_acquire_timeout.
$CLICKHOUSE_CLIENT --query "
SELECT name FROM system.parts WHERE database = '$ORDINARY_DB' AND table = 't_parts_lock_wait'
FORMAT Null
SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break', lock_acquire_timeout = 60,
         log_comment = '$QUERY_LOG_COMMENT';
"

# `lock_acquire_timeout = 0` means to wait indefinitely for the table lock. The query-status
# polling must still interrupt this infinite overall wait rather than passing zero into a single
# uninterruptible lock acquisition.
$CLICKHOUSE_CLIENT --query "
SELECT name FROM system.parts WHERE database = '$ORDINARY_DB' AND table = 't_parts_lock_wait'
FORMAT Null
SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break', lock_acquire_timeout = 0,
         log_comment = '$INFINITE_LOCK_QUERY_LOG_COMMENT';
"

# Let the DROP TABLE proceed without waiting out the whole reader sleep.
$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$READER_QUERY_ID' SYNC FORMAT Null"

wait $reader_pid $drop_pid 2>/dev/null || true

# The elapsed time is taken from `system.query_log` instead of being measured around the client
# invocation: the startup of `clickhouse-client` alone takes seconds in the debug and sanitizer
# builds, which is of the same order as the duration being asserted.
$CLICKHOUSE_CLIENT --query "
SYSTEM FLUSH LOGS query_log;

SELECT 'break queries returned early ' || toString(count() = 2 AND max(query_duration_ms) < 6000)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('$QUERY_LOG_COMMENT', '$INFINITE_LOCK_QUERY_LOG_COMMENT');

DROP DATABASE $ORDINARY_DB;
"
