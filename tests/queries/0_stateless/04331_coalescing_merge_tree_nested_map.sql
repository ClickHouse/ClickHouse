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

-- Exact #101937 repro: canonical `.key`/`.value` subcolumns with an Array(String) key, merged at
-- query time via SELECT ... FINAL (the case in the issue, distinct from OPTIMIZE FINAL above).
DROP TABLE IF EXISTS t_coalescing_nested_map_issue_101937;

CREATE TABLE t_coalescing_nested_map_issue_101937
(
    key UInt32,
    `testMap.key` Array(String),
    `testMap.value` Array(UInt32)
)
ENGINE = CoalescingMergeTree
ORDER BY key;

INSERT INTO t_coalescing_nested_map_issue_101937 VALUES (1, ['a'], [10]);
INSERT INTO t_coalescing_nested_map_issue_101937 VALUES (1, ['b'], [20]);

SELECT * FROM t_coalescing_nested_map_issue_101937 FINAL ORDER BY ALL;

DROP TABLE t_coalescing_nested_map_issue_101937;
