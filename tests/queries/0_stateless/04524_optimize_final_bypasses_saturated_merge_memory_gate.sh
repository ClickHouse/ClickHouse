#!/usr/bin/env bash
# Regression test for the merge memory reservation gate (see MergeMemoryReservation / StorageMergeTree).
# When running merges have reserved the whole merges_mutations_memory_usage_soft_limit, background merge
# selection is throttled by canEnqueueBackgroundTask, but a user-initiated OPTIMIZE must still proceed:
# it reserves unconditionally and must never be silently skipped by (or made to wait on) that gate.
# The merge_memory_reservation_gate_closed failpoint deterministically simulates a saturated gate (as if
# concurrent running merges had reserved the whole limit) so the bypass can be verified without racing
# real background merges. Each clickhouse-local invocation is its own process, so the failpoint does not
# leak into other tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "
    CREATE TABLE t_optimize_saturated_gate (k UInt64, v String) ENGINE = MergeTree ORDER BY k;

    -- Close the background admission gate up front, so nothing can merge the parts in the background.
    SYSTEM ENABLE FAILPOINT merge_memory_reservation_gate_closed;

    INSERT INTO t_optimize_saturated_gate SELECT number, toString(number) FROM numbers(1000);
    INSERT INTO t_optimize_saturated_gate SELECT number, toString(number) FROM numbers(1000, 1000);
    INSERT INTO t_optimize_saturated_gate SELECT number, toString(number) FROM numbers(2000, 1000);

    -- With the gate forced closed, this OPTIMIZE used to be rejected (CANNOT_SELECT) or silently no-op.
    -- It must now bypass the gate and merge everything down to a single part, or throw - never no-op.
    OPTIMIZE TABLE t_optimize_saturated_gate FINAL SETTINGS optimize_throw_if_noop = 1;

    SYSTEM DISABLE FAILPOINT merge_memory_reservation_gate_closed;

    SELECT count() FROM t_optimize_saturated_gate;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_optimize_saturated_gate' AND active;
"
