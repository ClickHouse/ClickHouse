#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-ordinary-database, no-replicated-database
# Tag no-parallel: the failpoint is server wide and makes background scheduling throw for every transactional table
# Tag no-fasttest: transactions need the Keeper backed transaction log
# Tag no-ordinary-database: transactions require an Atomic database
# Tag no-replicated-database: the test drives one local table's own background scheduling

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FAILPOINT="mt_throw_after_background_transaction_begin"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FAILPOINT" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_txn_unwind" 2>/dev/null || true
}

trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_txn_unwind (n UInt64) ENGINE = MergeTree ORDER BY tuple()"

# A transactional insert is what turns on this table's transactions_enabled flag, so that every later
# background scheduling tick begins a transaction and holds it in a MergeTreeTransactionHolder.
$CLICKHOUSE_CLIENT -q "
    BEGIN TRANSACTION;
    INSERT INTO t_txn_unwind SETTINGS async_insert = 0 VALUES (1);
    COMMIT;
"

# Precondition: that insert has to have been transactional, otherwise the holder is never
# constructed and everything below would pass without exercising the path at all. A part written
# outside a transaction carries the nil host id in its creation_tid.
$CLICKHOUSE_CLIENT -q "
    SELECT 'precondition', count() = 1
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_txn_unwind' AND active
      AND creation_tid.3 != toUUID('00000000-0000-0000-0000-000000000000')
"

# Only rows logged after this point count, so repeated runs against one server (the flaky check runs
# this test 50 times) cannot see each other. UTC on both sides, because the session timezone is randomized.
START=$($CLICKHOUSE_CLIENT -q "SELECT toString(now64(6, 'UTC'))")

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FAILPOINT"

# An insert wakes the background jobs assignee, which now throws while the transaction holder is
# alive, so the holder is destroyed during unwinding and rolls the transaction back from there.
$CLICKHOUSE_CLIENT -q "INSERT INTO t_txn_unwind SETTINGS async_insert = 0 VALUES (2)"

# The assignee's catch (...) reports the injected exception at Error level, which is the witness that
# the unwinding path was both reached and survived. The failpoint is server wide, so the witness is keyed
# to this table: any other transactional table left on a reused server throws the very same exception.
# text_log keeps the exception's format string and its arguments in their own columns, so the table is
# matched exactly rather than as a substring of the rendered message.
injected=0
for _ in {1..60}; do
    $CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"

    injected=$($CLICKHOUSE_CLIENT -q "
        SELECT count() > 0
        FROM system.text_log
        WHERE event_time_microseconds > toDateTime64('$START', 6, 'UTC')
          AND logger_name LIKE '%BackgroundJobsAssignee%'
          AND message_format_string = 'Injected failure after beginning a background transaction for {}'
          AND value1 = currentDatabase() || '.t_txn_unwind'
    ")

    if [ "$injected" = "1" ]; then
        break
    fi

    sleep 0.5
done

echo "injected $injected"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FAILPOINT"

# The server has to still be serving queries, and both committed rows have to still be there.
$CLICKHOUSE_CLIENT -q "SELECT 'alive', 1"
$CLICKHOUSE_CLIENT -q "SELECT 'rows', count() FROM t_txn_unwind"
