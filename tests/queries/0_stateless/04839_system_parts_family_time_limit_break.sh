#!/usr/bin/env bash
# Tags: no-ordinary-database, no-parallel
# - no-ordinary-database: `system.dropped_tables_parts` is filled only for Atomic databases,
#   where a dropped table is kept for `database_atomic_delay_before_drop_table_sec`.
# - no-parallel: the test switches the `slowdown_system_parts_enumeration` failpoint, which is
#   a server-global switch, so a concurrently running instance of this test could turn it off
#   in the middle of the checks.
#
# Tests that the eager readers of the `system.parts` family honor `max_execution_time` with
# `timeout_overflow_mode = 'break'`: the query stops early and returns the rows collected so far
# instead of failing.
#
# Both groups of checks run under the `slowdown_system_parts_enumeration` failpoint, which only
# affects the tables named `t_slowdown_system_parts*` (see below).
#
# The first group of checks uses a deadline of 1 millisecond, which is guaranteed to fire before
# the full result is built, because the failpoint sleeps 500 ms on every enumerated part: every
# query must return fewer rows than the full result. Without the failpoint this assertion would be
# racy: a fast machine can build the whole 20-row result in under a millisecond, before the first
# cancellation checkpoint sees an expired deadline. Only the upper bound is asserted, because the
# exact number of rows collected before the deadline is inherently nondeterministic.
#
# The second group of checks proves that the cancellation checkpoints actually stop the eager
# result building quickly, with timed assertions:
# - the per-part checkpoints: the failpoint sleeps 500 ms on every enumerated part, so building
#   the full result of a 20-part table takes at least 10 seconds;
# - the per-column checkpoints of `system.parts_columns` and `system.projection_parts_columns`:
#   the failpoint sleeps 1 second per `COLUMNS_CANCELLATION_CHECK_PERIOD` (128) enumerated
#   columns of a part, so building the full result over a single part with 641 columns takes
#   at least 5 seconds;
# - the stop callback inside the parts-snapshot walks of MergeTree: for the tables with the
#   '_snap' name marker the failpoint sleeps 500 ms per enumerated part inside the walk itself
#   and polls the callback on every part (its regular cadence of 8192 parts cannot be reached
#   with a fixture of a reasonable size);
# - the checkpoints of the column-metadata prepass of the column-oriented tables: for the tables
#   with the '_meta' name marker the failpoint sleeps 1 second per 128 enumerated metadata
#   columns inside the prepass.
# In all cases a query with a 1 second deadline must finish well under the full build time,
# and it can only do so by stopping at the checkpoints. Without the checkpoints these queries
# keep building rows long past the deadline and the elapsed time assertions fail.
#
# The elapsed time is taken from `system.query_log`, not measured around the client invocation:
# the startup of `clickhouse-client` alone takes seconds in the debug and sanitizer builds, which
# is of the same order as the durations being asserted. For the same reason all the timed queries
# are sent in a single client invocation.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A table with several parts and a projection: exercises the checkpoints of the per-part loops.
NUM_PARTS=20
# This fixture exercises the eager storage-discovery pass in `StoragesDroppedInfoStream`.
NUM_DROPPED_TABLES=20
# A table with many columns in a single part: exercises the checkpoints of the column-enumeration
# loops, which fire every 128 enumerated columns: 641 columns give 5 checkpoints per part.
NUM_WIDE_COLUMNS=640
# A query that stops at the first checkpoint after the deadline takes about a second, while the
# full result takes at least five seconds to build.
MAX_DURATION_MS=3000

WIDE_COLUMNS=$(for i in $(seq 1 $NUM_WIDE_COLUMNS); do echo -n ", c$i UInt64"; done)
DROPPED_DISCOVERY_TABLES=$(for i in $(seq 1 $NUM_DROPPED_TABLES); do echo "
CREATE TABLE t_slowdown_system_parts_dropped_discovery_$i (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_slowdown_system_parts_dropped_discovery_$i VALUES (1);
DROP TABLE t_slowdown_system_parts_dropped_discovery_$i SETTINGS database_atomic_wait_for_drop_and_detach_synchronously = 0;"; done)

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_slowdown_system_parts;
DROP TABLE IF EXISTS t_slowdown_system_parts_dropped;
DROP TABLE IF EXISTS t_slowdown_system_parts_wide;
DROP TABLE IF EXISTS t_slowdown_system_parts_snap;
DROP TABLE IF EXISTS t_slowdown_system_parts_meta;
DROP TABLE IF EXISTS t_break_result;

CREATE TABLE t_slowdown_system_parts (x UInt64, PROJECTION p (SELECT x ORDER BY x))
ENGINE = MergeTree ORDER BY x PARTITION BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

CREATE TABLE t_slowdown_system_parts_dropped (x UInt64)
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

-- The '_meta' name marker additionally slows down the column-metadata prepass of the
-- column-oriented tables, so the timed checks can prove that the prepass checkpoints stop it.
CREATE TABLE t_slowdown_system_parts_meta (x UInt64 $WIDE_COLUMNS, PROJECTION pm (SELECT * ORDER BY x))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

CREATE TABLE t_break_result (name String) ENGINE = Memory;

INSERT INTO t_slowdown_system_parts SELECT number FROM numbers($NUM_PARTS) SETTINGS max_partitions_per_insert_block = 0;
INSERT INTO t_slowdown_system_parts_dropped SELECT number FROM numbers($NUM_PARTS) SETTINGS max_partitions_per_insert_block = 0;
INSERT INTO t_slowdown_system_parts_wide (x) VALUES (1);
INSERT INTO t_slowdown_system_parts_snap SELECT number FROM numbers($NUM_PARTS) SETTINGS max_partitions_per_insert_block = 0;
INSERT INTO t_slowdown_system_parts_meta (x) VALUES (1);
$DROPPED_DISCOVERY_TABLES

-- Sanity check: without any limits, the whole result is built.
SELECT 'full columns', count() = $NUM_WIDE_COLUMNS + 1 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_slowdown_system_parts_wide';
SELECT 'full dropped discovery', count() = $NUM_DROPPED_TABLES FROM system.dropped_tables_parts WHERE database = currentDatabase() AND table LIKE 't_slowdown_system_parts_dropped_discovery_%';

-- The table is dropped but kept in system.dropped_tables_parts for database_atomic_delay_before_drop_table_sec.
DROP TABLE t_slowdown_system_parts_dropped SETTINGS database_atomic_wait_for_drop_and_detach_synchronously = 0;
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
        WHERE database = currentDatabase() AND table LIKE 't_slowdown_system_parts_dropped_discovery_%'
        SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'break';

    SELECT 'quick dropped_tables_parts_discovery', count() < $NUM_DROPPED_TABLES FROM t_break_result;
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
    check_break_query dropped_tables_parts dropped_tables_parts t_slowdown_system_parts_dropped $NUM_PARTS
    check_break_dropped_discovery
} | $CLICKHOUSE_CLIENT

# $1 - the position of the check, $2 - a label, $3 - the system table, $4 - the source table,
# $5 - the select list (default: name).
# With the failpoint enabled, building the full result takes at least five seconds, and a query
# with a 1 second deadline must stop at the checkpoints and finish much earlier. The elapsed time
# of every query is looked up afterwards in `system.query_log` by its `log_comment`, which also
# gives the checks their deterministic output order.
function timed_query()
{
    echo "
    SELECT ${5:-name} FROM system.$3 WHERE database = currentDatabase() AND table = '$4'
    FORMAT Null
    SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break', log_comment = 'timed_04839_$1 $2';
    "
}

function timed_dropped_discovery()
{
    echo "
    SELECT name FROM system.dropped_tables_parts
    WHERE database = currentDatabase() AND table LIKE 't_slowdown_system_parts_dropped_discovery_%'
    FORMAT Null
    SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break', log_comment = 'timed_04839_08 dropped_tables_parts_discovery';
    "
}

{
    timed_query 01 parts parts t_slowdown_system_parts
    timed_query 02 parts_columns parts_columns t_slowdown_system_parts
    timed_query 03 projection_parts projection_parts t_slowdown_system_parts
    timed_query 04 projection_parts_columns projection_parts_columns t_slowdown_system_parts
    timed_query 05 parts_columns_wide parts_columns t_slowdown_system_parts_wide
    timed_query 06 projection_parts_columns_wide projection_parts_columns t_slowdown_system_parts_wide
    timed_query 07 dropped_tables_parts dropped_tables_parts t_slowdown_system_parts_dropped

    # The failpoint delays each of 20 entries while `StoragesDroppedInfoStream` eagerly discovers
    # dropped storages. This is before inherited per-part enumeration, so it pins the prepass poll.
    timed_dropped_discovery

    # The '_snap' fixture times out inside the parts-snapshot walk in MergeTree (500 ms per part,
    # at least 10 seconds for the full walk). Selecting the _state column switches to the walks
    # over all part states (getAllDataPartsVector / getAllProjectionPartsVector instead of the
    # ForInternalUsage helpers), so both pairs of helpers are covered.
    timed_query 09 parts_snap parts t_slowdown_system_parts_snap
    timed_query 10 parts_snap_state parts t_slowdown_system_parts_snap 'name, _state'
    timed_query 11 projection_parts_snap projection_parts t_slowdown_system_parts_snap
    timed_query 12 projection_parts_snap_state projection_parts t_slowdown_system_parts_snap 'name, _state'

    # The '_meta' fixture times out inside the column-metadata prepass (1 second per 128 enumerated
    # metadata columns, at least 5 seconds for the full prepass over 641 columns).
    timed_query 13 parts_columns_meta parts_columns t_slowdown_system_parts_meta
    timed_query 14 projection_parts_columns_meta projection_parts_columns t_slowdown_system_parts_meta
} | $CLICKHOUSE_CLIENT

disable_slowdown_failpoint

$CLICKHOUSE_CLIENT --query "
SYSTEM FLUSH LOGS query_log;

SELECT 'fast ' || splitByChar(' ', log_comment)[2] || ' ' || toString(query_duration_ms < $MAX_DURATION_MS)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND startsWith(log_comment, 'timed_04839_')
ORDER BY log_comment;

DROP TABLE t_break_result;
DROP TABLE t_slowdown_system_parts_wide;
DROP TABLE t_slowdown_system_parts_snap;
DROP TABLE t_slowdown_system_parts_meta;
DROP TABLE t_slowdown_system_parts;
"
