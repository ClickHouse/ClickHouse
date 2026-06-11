-- A Nested `xxxMap` column in CoalescingMergeTree previously aborted during merge (the column was
-- detected as a map candidate, skipped, and left unaggregated, tripping a null-column assertion).
-- Coalescing has no by-key merge (that is a SummingMergeTree concept), so the map's arrays are
-- coalesced as whole columns like any other column: the last value wins.

DROP TABLE IF EXISTS t_coalescing_nested_map;

CREATE TABLE t_coalescing_nested_map
(
    k UInt32,
    statsMap Nested(id UInt32, val UInt64)
)
ENGINE = CoalescingMergeTree
ORDER BY k;

INSERT INTO t_coalescing_nested_map VALUES (1, [1, 2], [10, 20]);
INSERT INTO t_coalescing_nested_map VALUES (1, [1, 3], [100, 200]);

OPTIMIZE TABLE t_coalescing_nested_map FINAL;

SELECT k, statsMap.id, statsMap.val FROM t_coalescing_nested_map ORDER BY k;

DROP TABLE t_coalescing_nested_map;
