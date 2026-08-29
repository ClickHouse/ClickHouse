#!/usr/bin/env bash
# Tags: no-ordinary-database, no-parallel
# - no-ordinary-database: `system.dropped_tables_parts` is filled only for Atomic databases,
#   where a dropped table is kept for `database_atomic_delay_before_drop_table_sec`.
# - no-parallel: the test switches the `slowdown_system_parts_enumeration` failpoint, which is
#   a server-global switch, so a concurrently running instance of this test could turn it off
#   in the middle of the checks.
#
# Tests that the eager readers of the `system.parts` family honor `max_execution_time` with
# `timeout_overflow_mode = 'break'`: the query stops the eager result building at the first
# cancellation checkpoint that sees the expired deadline and finishes without an error. No partial
# prefix is handed out past the deadline (see the note before the second group below), so the
# checks assert that the work stops quickly, not that the collected rows are returned.
#
# Both groups of checks run under the `slowdown_system_parts_enumeration` failpoint, which only
# affects the tables named `t_slowdown_system_parts*` (see below).
#
# The first group of checks uses a deadline of 1 millisecond, which is guaranteed to fire before
# the full result is built, because the failpoint sleeps 500 ms on every enumerated part: every
# query must return fewer rows than the full result. Without the failpoint this assertion would be
# racy: a fast machine can build the whole 10-row result in under a millisecond, before the first
# cancellation checkpoint sees an expired deadline. Only the upper bound is asserted, because the
# exact number of rows collected before the deadline is inherently nondeterministic.
#
# The second group of checks proves that the cancellation checkpoints actually stop the eager
# result building quickly. Every sleep of the failpoint is counted by the
# `SystemPartsEnumerationSlowdownSleeps` profile event, and each check asserts an upper bound on
# the number of sleeps its query performed before stopping, taken from `system.query_log`. The
# sleeps covered are:
# - the per-part checkpoints: the failpoint sleeps 500 ms on every enumerated part, so building
#   the full result of a 10-part table performs 10 sleeps;
# - the per-column checkpoints of `system.parts_columns` and `system.projection_parts_columns`:
#   the failpoint sleeps 500 ms per `COLUMNS_CANCELLATION_CHECK_PERIOD` (128) enumerated
#   columns of a part, so building the full result over a single part with 1025 columns performs
#   8 sleeps;
# - the stop callback inside the parts-snapshot walks of MergeTree: for the tables with the
#   '_snap' name marker the failpoint sleeps 500 ms per enumerated part inside the walk itself
#   and polls the callback on every part (its regular cadence of 8192 parts cannot be reached
#   with a fixture of a reasonable size);
# - the checkpoint of the database/table discovery walk in `StoragesInfoStream`: for the tables
#   with the '_discovery' name marker the failpoint sleeps 500 ms per walked table, and the
#   fixture lives in a dedicated database so that the sleeps do not slow down the walk for the
#   other checks;
# - the checkpoints of the column-metadata prepass of the column-oriented tables: for the tables
#   with the '_meta' name marker the failpoint sleeps 500 ms per 128 enumerated metadata
#   columns inside the prepass;
# - the per-column checkpoints of the column-oriented tables after the parts snapshot itself has
#   already stopped: the '_snap_wide' fixture combines the slowed down snapshot walk with parts
#   that have many columns, so the row materialization that follows the stopped snapshot must keep
#   polling instead of enumerating every column of every returned part.
# In all cases a query with a 0.5 second deadline must stop at the first checkpoint that sees the
# expired deadline. Every sleep site is polled at the same cadence as it sleeps, so a query that
# honors the checkpoints performs at most 2-3 sleeps per exercised site before it stops, while a
# query that ignores them performs one sleep per enumerated element: at least 8 in every check
# below. The sleep count is asserted instead of the elapsed time on purpose: the count has a
# deterministic upper bound, while any wall-clock bound flakes on a loaded worker, where the
# system-table query pipeline alone can take seconds to initialize. Load can only make the
# deadline fire earlier and the count smaller, never larger.
#
# Note that a query of these tables that runs into its deadline returns no rows at all, in any
# overflow mode: the whole result is built as a single chunk after the enumeration, and a chunk
# produced past the deadline is never handed over to the rest of the pipeline. So the checks below
# assert that the work stops quickly, not that a partial result is handed out.
#
# All the checked queries are sent in a single client invocation: the client startup alone takes
# seconds in the debug and sanitizer builds.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A table with several parts and a projection: exercises the checkpoints of the per-part loops.
NUM_PARTS=10
# This fixture exercises the eager storage-discovery pass in `StoragesDroppedInfoStream`.
NUM_DROPPED_TABLES=10
# This fixture exercises the eager database/table discovery walk in `StoragesInfoStream`.
# It lives in a separate database: the walk enumerates every table of the walked database
# regardless of the query's table filter, so keeping these tables next to the other fixtures
# would add discovery sleeps to every other check.
NUM_DISCOVERY_TABLES=10
# A table with many columns in a single part: exercises the checkpoints of the column-enumeration
# loops, which fire every 128 enumerated columns: 1025 columns give 8 checkpoints per part.
NUM_WIDE_COLUMNS=1024
# The upper bound on the failpoint sleeps a query may perform before it stops. A query that stops
# at the checkpoints performs at most 2-3 sleeps: the sleeps are 500 ms each, the deadline is
# 0.5 seconds, and every sleep site is polled at the sleep cadence, so at most two sleeps fit
# before the deadline and at most one checkpoint sees it late. A query that ignores a checkpoint
# performs one sleep per enumerated element instead: at least 8 in every check below.
MAX_SLEEPS=4

WIDE_COLUMNS=$(for i in $(seq 1 $NUM_WIDE_COLUMNS); do echo -n ", c$i UInt64"; done)
TEST_RUN_SUFFIX="${CLICKHOUSE_TEST_UNIQUE_NAME}_$$"
DROPPED_TABLE="t_slowdown_system_parts_dropped_${TEST_RUN_SUFFIX}"
DROPPED_DISCOVERY_TABLE_PREFIX="t_slowdown_system_parts_dropped_discovery_${TEST_RUN_SUFFIX}_"
QUERY_LOG_PREFIX="timed_04839_${TEST_RUN_SUFFIX}"
DROPPED_DISCOVERY_TABLES=$(for i in $(seq 1 $NUM_DROPPED_TABLES); do echo "
CREATE TABLE ${DROPPED_DISCOVERY_TABLE_PREFIX}$i (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${DROPPED_DISCOVERY_TABLE_PREFIX}$i VALUES (1);
DROP TABLE ${DROPPED_DISCOVERY_TABLE_PREFIX}$i SETTINGS database_atomic_wait_for_drop_and_detach_synchronously = 0;"; done)
# The '_discovery' name marker slows down the database/table discovery walk in
# `StoragesInfoStream` (500 ms per walked table), so the checks over this database prove that
# the walk stops at its cancellation checkpoint instead of enumerating every table.
DISCOVERY_DATABASE="${CLICKHOUSE_DATABASE}_discovery"
DISCOVERY_TABLES=$(for i in $(seq 1 $NUM_DISCOVERY_TABLES); do echo "
CREATE TABLE $DISCOVERY_DATABASE.t_slowdown_system_parts_discovery_$i (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO $DISCOVERY_DATABASE.t_slowdown_system_parts_discovery_$i VALUES (1);"; done)

$CLICKHOUSE_CLIENT --query "
DROP DATABASE IF EXISTS $DISCOVERY_DATABASE;
CREATE DATABASE $DISCOVERY_DATABASE;
$DISCOVERY_TABLES

DROP TABLE IF EXISTS t_slowdown_system_parts;
DROP TABLE IF EXISTS $DROPPED_TABLE;
DROP TABLE IF EXISTS t_slowdown_system_parts_wide;
DROP TABLE IF EXISTS t_slowdown_system_parts_snap;
DROP TABLE IF EXISTS t_slowdown_system_parts_snap_wide;
DROP TABLE IF EXISTS t_slowdown_system_parts_meta;
DROP TABLE IF EXISTS t_break_result;

CREATE TABLE t_slowdown_system_parts (x UInt64, PROJECTION p (SELECT x ORDER BY x))
ENGINE = MergeTree ORDER BY x PARTITION BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

CREATE TABLE $DROPPED_TABLE (x UInt64)
ENGINE = MergeTree ORDER BY x PARTITION BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

CREATE TABLE t_slowdown_system_parts_wide (x UInt64 $WIDE_COLUMNS, PROJECTION pw (SELECT * ORDER BY x))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

-- The '_snap' name marker additionally slows down the parts-snapshot walks inside MergeTree
-- (the helpers behind StoragesInfo.getParts / StoragesInfo.getProjectionParts) and makes them
-- poll the stop callback on every enumerated part, so the timed checks can prove that the
-- snapshot materialization itself is interruptible.
CREATE TABLE t_slowdown_system_parts_snap (x UInt64, PROJECTION p (SELECT x ORDER BY x))
ENGINE = MergeTree ORDER BY x PARTITION BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

-- The '_snap_wide' name marker combines the slowed down snapshot walk with a part that has many
-- columns: the snapshot stops first, and the per-column checkpoints must still stop the row
-- materialization that follows it.
CREATE TABLE t_slowdown_system_parts_snap_wide (x UInt64 $WIDE_COLUMNS, PROJECTION pw (SELECT * ORDER BY x))
ENGINE = MergeTree ORDER BY x PARTITION BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

-- The '_meta' name marker additionally slows down the column-metadata prepass of the
-- column-oriented tables, so the timed checks can prove that the prepass checkpoints stop it.
CREATE TABLE t_slowdown_system_parts_meta (x UInt64 $WIDE_COLUMNS, PROJECTION pm (SELECT * ORDER BY x))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

CREATE TABLE t_break_result (name String) ENGINE = Memory;

INSERT INTO t_slowdown_system_parts SELECT number FROM numbers($NUM_PARTS) SETTINGS max_partitions_per_insert_block = 0;
INSERT INTO $DROPPED_TABLE SELECT number FROM numbers($NUM_PARTS) SETTINGS max_partitions_per_insert_block = 0;
INSERT INTO t_slowdown_system_parts_wide (x) VALUES (1);
INSERT INTO t_slowdown_system_parts_snap SELECT number FROM numbers($NUM_PARTS) SETTINGS max_partitions_per_insert_block = 0;
INSERT INTO t_slowdown_system_parts_snap_wide (x) SELECT number FROM numbers(2) SETTINGS max_partitions_per_insert_block = 0;
INSERT INTO t_slowdown_system_parts_meta (x) VALUES (1);
$DROPPED_DISCOVERY_TABLES

-- Sanity check: without any limits, the whole result is built.
SELECT 'full columns', count() = $NUM_WIDE_COLUMNS + 1 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_slowdown_system_parts_wide';
SELECT 'full dropped discovery', count() = $NUM_DROPPED_TABLES FROM system.dropped_tables_parts WHERE database = currentDatabase() AND table LIKE '${DROPPED_DISCOVERY_TABLE_PREFIX}%';
SELECT 'full installed discovery', count() = $NUM_DISCOVERY_TABLES FROM system.parts WHERE database = '$DISCOVERY_DATABASE';

-- The table is dropped but kept in system.dropped_tables_parts for database_atomic_delay_before_drop_table_sec.
DROP TABLE $DROPPED_TABLE SETTINGS database_atomic_wait_for_drop_and_detach_synchronously = 0;
"

# $1 - a label, $2 - the system table, $3 - the source table, $4 - the total number of rows without the time limit.
function check_break_query()
{
    echo "
    TRUNCATE TABLE t_break_result;

    INSERT INTO t_break_result
        SELECT name FROM system.$2 WHERE database = currentDatabase() AND table = '$3'
        SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'break';

    SELECT 'quick $1', count() < $4 FROM t_break_result;
    "
}

function check_break_dropped_discovery()
{
    echo "
    TRUNCATE TABLE t_break_result;

    INSERT INTO t_break_result
        SELECT name FROM system.dropped_tables_parts
        WHERE database = currentDatabase() AND table LIKE '${DROPPED_DISCOVERY_TABLE_PREFIX}%'
        SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'break';

    SELECT 'quick dropped_tables_parts_discovery', count() < $NUM_DROPPED_TABLES FROM t_break_result;
    "
}

function check_break_installed_discovery()
{
    echo "
    TRUNCATE TABLE t_break_result;

    INSERT INTO t_break_result
        SELECT name FROM system.parts WHERE database = '$DISCOVERY_DATABASE'
        SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'break';

    SELECT 'quick parts_installed_discovery', count() < $NUM_DISCOVERY_TABLES FROM t_break_result;
    "
}

function disable_slowdown_failpoint()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT slowdown_system_parts_enumeration" > /dev/null 2>&1 ||:
}

trap disable_slowdown_failpoint EXIT

# The failpoint must be enabled after the sanity check above, which builds the full result
# of the wide table and would take minutes with the per-column sleeps.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT slowdown_system_parts_enumeration"

# All quick checks go in a single client invocation: the client startup is significant
# with the sanitizer and debug builds.
{
    check_break_query parts parts t_slowdown_system_parts $NUM_PARTS
    check_break_query parts_columns parts_columns t_slowdown_system_parts $NUM_PARTS
    check_break_query projection_parts projection_parts t_slowdown_system_parts $NUM_PARTS
    check_break_query projection_parts_columns projection_parts_columns t_slowdown_system_parts $NUM_PARTS
    check_break_query parts_columns_wide parts_columns t_slowdown_system_parts_wide $((NUM_WIDE_COLUMNS + 1))
    check_break_query dropped_tables_parts dropped_tables_parts $DROPPED_TABLE $NUM_PARTS
    check_break_dropped_discovery
    check_break_installed_discovery
} | $CLICKHOUSE_CLIENT

# $1 - the position of the check, $2 - a label, $3 - the system table, $4 - the source table,
# $5 - the select list (default: name).
# The number of failpoint sleeps of every query is looked up afterwards in `system.query_log` by
# its `log_comment`, which also gives the checks their deterministic output order.
function counted_query()
{
    echo "
    SELECT ${5:-name} FROM system.$3 WHERE database = currentDatabase() AND table = '$4'
    FORMAT Null
    SETTINGS max_execution_time = 0.5, timeout_overflow_mode = 'break',
             log_comment = '$QUERY_LOG_PREFIX $1 $2';
    "
}

function counted_installed_discovery()
{
    echo "
    SELECT name FROM system.parts WHERE database = '$DISCOVERY_DATABASE'
    FORMAT Null
    SETTINGS max_execution_time = 0.5, timeout_overflow_mode = 'break',
             log_comment = '$QUERY_LOG_PREFIX 17 parts_installed_discovery';
    "
}

function counted_dropped_discovery()
{
    echo "
    SELECT name FROM system.dropped_tables_parts
    WHERE database = currentDatabase() AND table LIKE '${DROPPED_DISCOVERY_TABLE_PREFIX}%'
    FORMAT Null
    SETTINGS max_execution_time = 0.5, timeout_overflow_mode = 'break',
             log_comment = '$QUERY_LOG_PREFIX 08 dropped_tables_parts_discovery';
    "
}

{
    counted_query 01 parts parts t_slowdown_system_parts
    counted_query 02 parts_columns parts_columns t_slowdown_system_parts
    counted_query 03 projection_parts projection_parts t_slowdown_system_parts
    counted_query 04 projection_parts_columns projection_parts_columns t_slowdown_system_parts
    counted_query 05 parts_columns_wide parts_columns t_slowdown_system_parts_wide
    counted_query 06 projection_parts_columns_wide projection_parts_columns t_slowdown_system_parts_wide
    counted_query 07 dropped_tables_parts dropped_tables_parts $DROPPED_TABLE

    # The failpoint delays each of the entries while `StoragesDroppedInfoStream` eagerly discovers
    # dropped storages. This is before inherited per-part enumeration, so it pins the prepass poll.
    counted_dropped_discovery

    # The '_snap' fixture runs into its deadline inside the parts-snapshot walk in MergeTree
    # (500 ms per part, one sleep per part for the full walk). Selecting the _state column switches
    # to the walks over all part states (getAllDataPartsVector / getAllProjectionPartsVector
    # instead of the ForInternalUsage helpers), so both pairs of helpers are covered.
    counted_query 09 parts_snap parts t_slowdown_system_parts_snap
    counted_query 10 parts_snap_state parts t_slowdown_system_parts_snap 'name, _state'
    counted_query 11 projection_parts_snap projection_parts t_slowdown_system_parts_snap
    counted_query 12 projection_parts_snap_state projection_parts t_slowdown_system_parts_snap 'name, _state'

    # The '_meta' fixture runs into its deadline inside the column-metadata prepass (500 ms per
    # 128 enumerated metadata columns, 8 sleeps for the full prepass over 1025 columns).
    counted_query 13 parts_columns_meta parts_columns t_slowdown_system_parts_meta
    counted_query 14 projection_parts_columns_meta projection_parts_columns t_slowdown_system_parts_meta

    # The '_snap_wide' fixture stops inside the parts-snapshot walk, and the parts it returns are
    # then materialized column by column: without the per-column checkpoints of that materialization
    # a single returned part alone performs at least eight more sleeps.
    counted_query 15 parts_columns_snap_wide parts_columns t_slowdown_system_parts_snap_wide
    counted_query 16 projection_parts_columns_snap_wide projection_parts_columns t_slowdown_system_parts_snap_wide

    # The '_discovery' fixture runs into its deadline inside the database/table discovery walk of
    # `StoragesInfoStream` (500 ms per walked table, one sleep per table for the full walk over
    # its dedicated database), so it pins the cancellation checkpoint of the walk itself.
    counted_installed_discovery
} | $CLICKHOUSE_CLIENT

disable_slowdown_failpoint

$CLICKHOUSE_CLIENT --query "
SYSTEM FLUSH LOGS query_log;

SELECT 'fast ' || any(label) || ' ' || toString(max(sleeps) <= $MAX_SLEEPS)
FROM
(
    SELECT
        splitByChar(' ', log_comment)[2] AS idx,
        splitByChar(' ', log_comment)[3] AS label,
        ProfileEvents['SystemPartsEnumerationSlowdownSleeps'] AS sleeps
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND startsWith(log_comment, '$QUERY_LOG_PREFIX ')
)
GROUP BY idx
ORDER BY idx;

DROP TABLE t_break_result;
DROP TABLE t_slowdown_system_parts_wide;
DROP TABLE t_slowdown_system_parts_snap;
DROP TABLE t_slowdown_system_parts_snap_wide;
DROP TABLE t_slowdown_system_parts_meta;
DROP TABLE t_slowdown_system_parts;
DROP DATABASE $DISCOVERY_DATABASE;
"
