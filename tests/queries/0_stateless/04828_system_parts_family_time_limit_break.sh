#!/usr/bin/env bash
# Tests that the eager readers of the `system.parts` family honor `max_execution_time` with
# `timeout_overflow_mode = 'break'`: the query stops early and returns the rows collected so far
# instead of failing, and it stops both between parts and while enumerating the columns of a part.
#
# The deadline below is smaller than the time it takes to even start executing a query, so it is
# guaranteed to fire: every query must return fewer rows than the full result. Without the
# cancellation checkpoints the whole result is built regardless of the deadline and the assertions
# fail. Only the upper bound is asserted, because the exact number of rows collected before the
# deadline is inherently nondeterministic - which checkpoint stops the query depends on the
# machine.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A table with many parts: exercises the checkpoints of the per-part loops.
NUM_PARTS=1000
# A table with many columns in a single part: exercises the checkpoints of the column-enumeration loops.
NUM_COLUMNS=3000

WIDE_COLUMNS=$(for i in $(seq 1 $NUM_COLUMNS); do echo -n ", c$i UInt64"; done)

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_break_parts;
DROP TABLE IF EXISTS t_break_columns;
DROP TABLE IF EXISTS t_break_result;

CREATE TABLE t_break_parts (x UInt64, PROJECTION p (SELECT x ORDER BY x))
ENGINE = MergeTree ORDER BY x PARTITION BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

CREATE TABLE t_break_columns (x UInt64 $WIDE_COLUMNS)
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

CREATE TABLE t_break_result (name String) ENGINE = Memory;

INSERT INTO t_break_parts SELECT number FROM numbers($NUM_PARTS) SETTINGS max_partitions_per_insert_block = 0;
INSERT INTO t_break_columns (x) VALUES (1);
"

# Sanity check: without any limits, the whole result is built.
$CLICKHOUSE_CLIENT --query "
SELECT 'full parts', count() = $NUM_PARTS FROM system.parts WHERE database = currentDatabase() AND table = 't_break_parts';
SELECT 'full columns', count() = $NUM_COLUMNS + 1 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_break_columns';
"

# $1 - the system table, $2 - the source table, $3 - the total number of rows without the time limit.
function check_break()
{
    $CLICKHOUSE_CLIENT --query "
    TRUNCATE TABLE t_break_result;

    INSERT INTO t_break_result
        SELECT name FROM system.$1 WHERE database = currentDatabase() AND table = '$2'
        SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'break';

    SELECT '$1', count() < $3 FROM t_break_result;
    "
}

check_break parts t_break_parts $NUM_PARTS
check_break parts_columns t_break_parts $NUM_PARTS
check_break projection_parts t_break_parts $NUM_PARTS
check_break projection_parts_columns t_break_parts $NUM_PARTS
check_break parts_columns t_break_columns $((NUM_COLUMNS + 1))

# `system.dropped_tables_parts` has no deterministic contents here, so it is only checked for not throwing.
$CLICKHOUSE_CLIENT --query "
SELECT * FROM system.dropped_tables_parts FORMAT Null SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'break';
"

$CLICKHOUSE_CLIENT --query "
DROP TABLE t_break_result;
DROP TABLE t_break_columns;
DROP TABLE t_break_parts;
"
