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

-- The `t2.value = 'Join_2_Value_0'` conjunct in the second ON clause references t2, the
-- null-supplying side of the first join. Only t1.id = 0 matches t2, so for t1.id in (1, 2)
-- the second join must not match t3, leaving t3.value empty.

SELECT 'shared predicate: default';
SELECT t1.id, t3.value
FROM t1
LEFT JOIN t2 ON t1.id = t2.id AND t1.value = 'Join_1_Value_0' AND t2.value = 'Join_2_Value_0'
LEFT JOIN t3 ON t2.id = t3.id AND t2.value = 'Join_2_Value_0' AND t3.value = 'Join_3_Value_0'
ORDER BY ALL;

SELECT 'shared predicate: dpsub';
SELECT t1.id, t3.value
FROM t1
LEFT JOIN t2 ON t1.id = t2.id AND t1.value = 'Join_1_Value_0' AND t2.value = 'Join_2_Value_0'
LEFT JOIN t3 ON t2.id = t3.id AND t2.value = 'Join_2_Value_0' AND t3.value = 'Join_3_Value_0'
ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub';

SELECT 'unshared predicate: default';
SELECT t1.id, t3.value
FROM t1
LEFT JOIN t2 ON t1.id = t2.id AND t1.value = 'Join_1_Value_0'
LEFT JOIN t3 ON t2.id = t3.id AND t2.value = 'Join_2_Value_0' AND t3.value = 'Join_3_Value_0'
ORDER BY ALL;

SELECT 'unshared predicate: dpsub';
SELECT t1.id, t3.value
FROM t1
LEFT JOIN t2 ON t1.id = t2.id AND t1.value = 'Join_1_Value_0'
LEFT JOIN t3 ON t2.id = t3.id AND t2.value = 'Join_2_Value_0' AND t3.value = 'Join_3_Value_0'
ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub';

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
