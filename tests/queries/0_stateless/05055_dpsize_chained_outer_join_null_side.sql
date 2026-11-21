SET enable_analyzer = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t3 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;

INSERT INTO t1 VALUES (0, 'Join_1_Value_0'), (1, 'Join_1_Value_1'), (2, 'Join_1_Value_2');
INSERT INTO t2 VALUES (0, 'Join_2_Value_0'), (1, 'Join_2_Value_1'), (3, 'Join_2_Value_3');
INSERT INTO t3 VALUES (0, 'Join_3_Value_0'), (1, 'Join_3_Value_1'), (4, 'Join_3_Value_4');

-- The dpsize variant of 04507_dpsub_chained_outer_join_null_side: single-table conjuncts of an
-- outer join ON clause are pinned to the null-supplying relation and must stay in that join's
-- ON condition even when the preserved side has already been joined with another relation.
-- dpsize currently refuses non-inner join steps and falls back to greedy, so greedy is kept in
-- the algorithm chain; once dpsize supports outer joins, this test guards its predicate placement.

SELECT 'inner then outer: dpsize';
SELECT t1.id, t3.value
FROM t1
JOIN t2 ON t1.id = t2.id
LEFT JOIN t3 ON t2.id = t3.id AND t2.value = 'Join_2_Value_0'
ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize,greedy';

SELECT 'shared predicate: dpsize';
SELECT t1.id, t3.value
FROM t1
LEFT JOIN t2 ON t1.id = t2.id AND t1.value = 'Join_1_Value_0' AND t2.value = 'Join_2_Value_0'
LEFT JOIN t3 ON t2.id = t3.id AND t2.value = 'Join_2_Value_0' AND t3.value = 'Join_3_Value_0'
ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize,greedy';

SELECT 'unshared predicate: dpsize';
SELECT t1.id, t3.value
FROM t1
LEFT JOIN t2 ON t1.id = t2.id AND t1.value = 'Join_1_Value_0'
LEFT JOIN t3 ON t2.id = t3.id AND t2.value = 'Join_2_Value_0' AND t3.value = 'Join_3_Value_0'
ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize,greedy';

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
