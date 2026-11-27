-- Tests that the DPsub join-order algorithm correctly reorders non-Inner (LEFT/RIGHT) joins.
-- For each shape we print the result twice: once with optimization disabled (the reference
-- order) and once with 'dpsub'. The two must be identical row-for-row; a wrong join kind or
-- orientation chosen by DPsub would change which rows get NULL-padded.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t3 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;

INSERT INTO t1 VALUES (0, 'v1_0'), (1, 'v1_1'), (2, 'v1_2');
INSERT INTO t2 VALUES (0, 'v2_0'), (1, 'v2_1'), (3, 'v2_3');
INSERT INTO t3 VALUES (0, 'v3_0'), (1, 'v3_1'), (4, 'v3_4');

-- Make t1 appear expensive so the optimizer prefers to join t2 and t3 first, exercising the
-- non-Inner reorder path (the null-supplying relation ends up at different relation indices).
SET param__internal_join_table_stat_hints = '{"t1": {"cardinality": 100000, "distinct_keys": {"id": 2}}, "t2": {"cardinality": 3, "distinct_keys": {"id": 3}}, "t3": {"cardinality": 3, "distinct_keys": {"id": 3}}}';
SET enable_analyzer = 1, single_join_prefer_left_table = 0;

SELECT 'LEFT+LEFT no opt:';
SELECT t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM t1 LEFT JOIN t2 ON t1.id = t2.id
LEFT JOIN t3 ON t2.id = t3.id ORDER BY ALL
SETTINGS query_plan_optimize_join_order_limit = 0;

SELECT 'LEFT+LEFT dpsub:';
SELECT t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM t1 LEFT JOIN t2 ON t1.id = t2.id
LEFT JOIN t3 ON t2.id = t3.id ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub';

SELECT 'RIGHT+RIGHT no opt:';
SELECT t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM t1 RIGHT JOIN t2 ON t1.id = t2.id
RIGHT JOIN t3 ON t2.id = t3.id ORDER BY ALL
SETTINGS query_plan_optimize_join_order_limit = 0;

SELECT 'RIGHT+RIGHT dpsub:';
SELECT t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM t1 RIGHT JOIN t2 ON t1.id = t2.id
RIGHT JOIN t3 ON t2.id = t3.id ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub';

SELECT 'RIGHT+LEFT no opt:';
SELECT t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM t1 RIGHT JOIN t2 ON t1.id = t2.id
LEFT JOIN t3 ON t2.id = t3.id ORDER BY ALL
SETTINGS query_plan_optimize_join_order_limit = 0;

SELECT 'RIGHT+LEFT dpsub:';
SELECT t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM t1 RIGHT JOIN t2 ON t1.id = t2.id
LEFT JOIN t3 ON t2.id = t3.id ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub';

SELECT 'LEFT+RIGHT no opt:';
SELECT t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM t1 LEFT JOIN t2 ON t1.id = t2.id
RIGHT JOIN t3 ON t2.id = t3.id ORDER BY ALL
SETTINGS query_plan_optimize_join_order_limit = 0;

SELECT 'LEFT+RIGHT dpsub:';
SELECT t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM t1 LEFT JOIN t2 ON t1.id = t2.id
RIGHT JOIN t3 ON t2.id = t3.id ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub';

-- Inner-join predicate that crosses an outer-join boundary, e.g.
-- `t1 LEFT JOIN t2 ON t1.x = t2.x JOIN t3 ON t2.y = t3.y AND t1.z = t3.z`.
-- Here `t1.z = t3.z` directly connects the preserved relation `t1` to `t3`, but it is pinned to
-- require `t2` (it sits above the LEFT JOIN boundary). A reorder algorithm may still treat `{t1}`
-- and `{t3}` as connected because of that binary edge, and try to join them first as an Inner join
-- with no applicable predicate (the pinned edge is filtered out). If such a plan reached the final
-- tree, the top join would apply `t1.x = t2.x` and `t2.y = t3.y` while dropping `t1.z = t3.z`,
-- producing extra rows. DPsub must return exactly the same rows as the unoptimized reference order.

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;

CREATE TABLE t1 (x UInt64, z UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t2 (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t3 (y UInt64, z UInt64) ENGINE = MergeTree ORDER BY y;

-- Rows are arranged so that `t1.x = t2.x` and `t2.y = t3.y` match while `t1.z = t3.z` does NOT for
-- the (x = 1) row, so the cross-boundary predicate is the only thing filtering it out. Dropping the
-- predicate would leak that row into the result.
INSERT INTO t1 VALUES (1, 100), (2, 300), (3, 500);
INSERT INTO t2 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO t3 VALUES (10, 999), (20, 300), (30, 500);

-- Make t2 (the null-supplying relation) look huge so the optimizer is tempted to defer joining it
-- and to build a t1/t3 join first, exercising the pinned-edge connectivity path.
SET param__internal_join_table_stat_hints = '{"t1": {"cardinality": 3, "distinct_keys": {"x": 3, "z": 3}}, "t2": {"cardinality": 1000000, "distinct_keys": {"x": 1, "y": 1}}, "t3": {"cardinality": 3, "distinct_keys": {"y": 3, "z": 3}}}';

SELECT 'LEFT cross-boundary no opt:';
SELECT t1.x, t1.z, t2.x, t2.y, t3.y, t3.z
FROM t1 LEFT JOIN t2 ON t1.x = t2.x
JOIN t3 ON t2.y = t3.y AND t1.z = t3.z ORDER BY ALL
SETTINGS query_plan_optimize_join_order_limit = 0;

SELECT 'LEFT cross-boundary dpsub:';
SELECT t1.x, t1.z, t2.x, t2.y, t3.y, t3.z
FROM t1 LEFT JOIN t2 ON t1.x = t2.x
JOIN t3 ON t2.y = t3.y AND t1.z = t3.z ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub';

SELECT 'RIGHT cross-boundary no opt:';
SELECT t1.x, t1.z, t2.x, t2.y, t3.y, t3.z
FROM t1 RIGHT JOIN t2 ON t1.x = t2.x
JOIN t3 ON t2.y = t3.y AND t1.z = t3.z ORDER BY ALL
SETTINGS query_plan_optimize_join_order_limit = 0;

SELECT 'RIGHT cross-boundary dpsub:';
SELECT t1.x, t1.z, t2.x, t2.y, t3.y, t3.z
FROM t1 RIGHT JOIN t2 ON t1.x = t2.x
JOIN t3 ON t2.y = t3.y AND t1.z = t3.z ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub';

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
