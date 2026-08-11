#!/usr/bin/env bash
# Tags: no-ordinary-database
# - no-ordinary-database: `system.dropped_tables_parts` is filled only for Atomic databases,
#   where a dropped table is kept for `database_atomic_delay_before_drop_table_sec`.
#
# Tests that the eager readers of the `system.parts` family honor `max_execution_time` with
# `timeout_overflow_mode = 'break'`: the query stops early and returns the rows collected so far
# instead of failing.
#
# The first group of checks uses a deadline smaller than the time it takes to even start executing
# a query, so it is guaranteed to fire: every query must return fewer rows than the full result.
# Only the upper bound is asserted, because the exact number of rows collected before the deadline
# is inherently nondeterministic.
#
# The second group of checks proves that the cancellation checkpoints actually stop the eager
# result building, with the `slowdown_system_parts_enumeration` failpoint, which only affects the
# tables named `t_slowdown_system_parts*`:
# - the per-part checkpoints: the failpoint sleeps 500 ms on every enumerated part, so building
#   the full result of a 20-part table takes at least 10 seconds;
# - the per-column checkpoints of `system.parts_columns` and `system.projection_parts_columns`:
#   the failpoint sleeps 1 second per `COLUMNS_CANCELLATION_CHECK_PERIOD` (128) enumerated
#   columns of a part, so building the full result over a single part with 1301 columns takes
#   at least 10 seconds.
# In both cases a query with a 1 second deadline must finish well under the full build time,
# and it can only do so by stopping at the checkpoints. Without the checkpoints these queries
# keep building rows long past the deadline and the elapsed time assertions fail.
# The failpoint only slows down the specially named tables, so concurrently running tests are not
# affected, and only upper bounds are asserted, so concurrently running instances of this test are
# not affected either.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A table with several parts and a projection: exercises the checkpoints of the per-part loops.
NUM_PARTS=20
# A table with many columns in a single part: exercises the checkpoints of the column-enumeration
# loops, which fire every 128 enumerated columns: 1301 columns give 10 checkpoints per part.
NUM_WIDE_COLUMNS=1300

WIDE_COLUMNS=$(for i in $(seq 1 $NUM_WIDE_COLUMNS); do echo -n ", c$i UInt64"; done)

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_slowdown_system_parts;
DROP TABLE IF EXISTS t_slowdown_system_parts_dropped;
DROP TABLE IF EXISTS t_slowdown_system_parts_wide;
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

CREATE TABLE t_break_result (name String) ENGINE = Memory;

INSERT INTO t_slowdown_system_parts SELECT number FROM numbers($NUM_PARTS) SETTINGS max_partitions_per_insert_block = 0;
INSERT INTO t_slowdown_system_parts_dropped SELECT number FROM numbers($NUM_PARTS) SETTINGS max_partitions_per_insert_block = 0;
INSERT INTO t_slowdown_system_parts_wide (x) VALUES (1);

-- Sanity check: without any limits, the whole result is built.
SELECT 'full columns', count() = $NUM_WIDE_COLUMNS + 1 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_slowdown_system_parts_wide';

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

# All quick checks go in a single client invocation: the client startup is significant
# with the sanitizer and debug builds.
{
    check_break_query parts parts t_slowdown_system_parts $NUM_PARTS
    check_break_query parts_columns parts_columns t_slowdown_system_parts $NUM_PARTS
    check_break_query projection_parts projection_parts t_slowdown_system_parts $NUM_PARTS
    check_break_query projection_parts_columns projection_parts_columns t_slowdown_system_parts $NUM_PARTS
    check_break_query parts_columns_wide parts_columns t_slowdown_system_parts_wide $((NUM_WIDE_COLUMNS + 1))
    check_break_query dropped_tables_parts dropped_tables_parts t_slowdown_system_parts_dropped $NUM_PARTS
} | $CLICKHOUSE_CLIENT

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT slowdown_system_parts_enumeration"

# $1 - a label, $2 - the system table, $3 - the source table.
# With the failpoint enabled, building the full result takes at least 10 seconds,
# and a query with a 1 second deadline must stop at the checkpoints and finish much earlier.
function check_fast()
{
    local start end
    start=$(date +%s)
    $CLICKHOUSE_CLIENT --query "
    SELECT name FROM system.$2 WHERE database = currentDatabase() AND table = '$3'
    FORMAT Null
    SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break';
    "
    end=$(date +%s)
    echo "fast $1 $((end - start < 6))"
}

check_fast parts parts t_slowdown_system_parts
check_fast parts_columns parts_columns t_slowdown_system_parts
check_fast projection_parts projection_parts t_slowdown_system_parts
check_fast projection_parts_columns projection_parts_columns t_slowdown_system_parts
check_fast parts_columns_wide parts_columns t_slowdown_system_parts_wide
check_fast projection_parts_columns_wide projection_parts_columns t_slowdown_system_parts_wide
check_fast dropped_tables_parts dropped_tables_parts t_slowdown_system_parts_dropped

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT slowdown_system_parts_enumeration"

$CLICKHOUSE_CLIENT --query "
DROP TABLE t_break_result;
DROP TABLE t_slowdown_system_parts_wide;
DROP TABLE t_slowdown_system_parts;
"
