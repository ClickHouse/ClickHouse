#!/usr/bin/env bash
# Regression test for the merge memory reservation gate (see MergeMemoryReservation).
# With a pathologically small merges_mutations_memory_usage_soft_limit every running merge saturates
# the reservation gate. Background merge selection is throttled, but an explicit OPTIMIZE ... FINAL
# reserves unconditionally, so it must still merge everything down to a single part - it used to be
# silently skipped when the gate was saturated by reservations of concurrent merges.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    CREATE TABLE t_optimize_under_soft_limit (k UInt64, v String) ENGINE = MergeTree ORDER BY k;

    SYSTEM STOP MERGES t_optimize_under_soft_limit;
    INSERT INTO t_optimize_under_soft_limit SELECT number, toString(number) FROM numbers(1000);
    INSERT INTO t_optimize_under_soft_limit SELECT number, toString(number) FROM numbers(1000, 1000);
    INSERT INTO t_optimize_under_soft_limit SELECT number, toString(number) FROM numbers(2000, 1000);
    SYSTEM START MERGES t_optimize_under_soft_limit;

    -- Must merge to a single part or throw, never no-op silently.
    OPTIMIZE TABLE t_optimize_under_soft_limit FINAL SETTINGS optimize_throw_if_noop = 1;

    SELECT count() FROM t_optimize_under_soft_limit;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_optimize_under_soft_limit' AND active;
" -- --merges_mutations_memory_usage_soft_limit=1
