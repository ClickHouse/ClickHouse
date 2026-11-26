-- `updated_header` decides which columns a mutation writes; for a compact or packed source part it
-- is the entire column list of the new part (`MutateTask.cpp:1012`), so a column dropped from it is
-- lost rather than hardlinked. The readonly subtraction must therefore never drop one there.
--
-- It cannot: that early return is gated on the source part being non-wide or non-full-storage, which
-- is a subset of the condition at `MutateTask.cpp:281-282` under which
-- `splitAndModifyMutationCommands` emits a `READ_COLUMN` for every column. Those land in a
-- non-readonly stage (`MutationsInterpreter.cpp:1571`), so every column is in the written set and
-- the subtraction is a no-op exactly where it would be dangerous. Nothing ties those two conditions
-- together, so this test pins the relationship rather than the fix that introduced the dependency on
-- it - it passes with and without that fix.
--
-- `b` is the column at risk: the index needs it, nothing changes it, and on a wide part it is
-- correctly dropped from `updated_header` and hardlinked instead.

DROP TABLE IF EXISTS t_compact_readonly_dependency;

CREATE TABLE t_compact_readonly_dependency
(
    id Int64,
    a Int64,
    b Int64,
    INDEX idx (a, b) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, index_granularity = 1;

INSERT INTO t_compact_readonly_dependency VALUES (1, 10, 100), (2, 20, 200);

-- Pins the premise: on a wide part the subtraction does drop `b`, and the assertions below would
-- then hold for the wrong reason.
SELECT part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_compact_readonly_dependency' AND active;

ALTER TABLE t_compact_readonly_dependency UPDATE a = a + 1 WHERE 1 SETTINGS mutations_sync = 2;

-- `b` must keep its stored value, not fall back to the type default.
SELECT id, a, b FROM t_compact_readonly_dependency ORDER BY id;
SELECT count(), countIf(b = 0) FROM t_compact_readonly_dependency;
-- Virtual columns are excluded: the runner randomizes `enable_block_number_column` and
-- `enable_block_offset_column`, which add `_block_number` / `_block_offset` to the part. They are
-- irrelevant here, and the assertion is that the mutation kept `a`, `b` and `id` - not that the part
-- has exactly three columns.
SELECT arraySort(groupArray(column)) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_compact_readonly_dependency' AND active
    AND NOT startsWith(column, '_');
CHECK TABLE t_compact_readonly_dependency SETTINGS check_query_single_value_result = 1;

DROP TABLE t_compact_readonly_dependency;
