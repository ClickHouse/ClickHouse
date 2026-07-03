DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t3 (id UInt64, value String) ENGINE = MergeTree ORDER BY id;

INSERT INTO t1 VALUES (0, 'v1_0'), (1, 'v1_1'), (2, 'v1_2');
INSERT INTO t2 VALUES (0, 'v2_0'), (1, 'v2_1'), (3, 'v2_3');
INSERT INTO t3 VALUES (0, 'v3_0'), (1, 'v3_1'), (4, 'v3_4');

-- Use dummy stats to make t1 appear expensive, forcing the optimizer
-- to join t2 and t3 first. This triggers the isValidJoinOrder code path
-- for non-Inner joins
SET param__internal_join_table_stat_hints = '{"t1": {"cardinality": 100000, "distinct_keys": {"id": 2}}, "t2": {"cardinality": 3, "distinct_keys": {"id": 3}}, "t3": {"cardinality": 3, "distinct_keys": {"id": 3}}}';
SET enable_analyzer = 1, single_join_prefer_left_table = 0;

SELECT 'RIGHT+LEFT no opt:';
SELECT t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM t1 RIGHT JOIN t2 ON t1.id = t2.id
LEFT JOIN t3 ON t2.id = t3.id ORDER BY ALL
SETTINGS query_plan_optimize_join_order_limit = 0;

SELECT 'RIGHT+LEFT greedy:';
SELECT t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM t1 RIGHT JOIN t2 ON t1.id = t2.id
LEFT JOIN t3 ON t2.id = t3.id ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy';

SELECT 'LEFT+LEFT no opt:';
SELECT t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM t1 LEFT JOIN t2 ON t1.id = t2.id
LEFT JOIN t3 ON t2.id = t3.id ORDER BY ALL
SETTINGS query_plan_optimize_join_order_limit = 0;

SELECT 'LEFT+LEFT greedy:';
SELECT t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM t1 LEFT JOIN t2 ON t1.id = t2.id
LEFT JOIN t3 ON t2.id = t3.id ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy';

SELECT 'RIGHT+RIGHT no opt:';
SELECT t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM t1 RIGHT JOIN t2 ON t1.id = t2.id
RIGHT JOIN t3 ON t2.id = t3.id ORDER BY ALL
SETTINGS query_plan_optimize_join_order_limit = 0;

SELECT 'RIGHT+RIGHT greedy:';
SELECT t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM t1 RIGHT JOIN t2 ON t1.id = t2.id
RIGHT JOIN t3 ON t2.id = t3.id ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy';

SELECT 'LEFT+RIGHT no opt:';
SELECT t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM t1 LEFT JOIN t2 ON t1.id = t2.id
RIGHT JOIN t3 ON t2.id = t3.id ORDER BY ALL
SETTINGS query_plan_optimize_join_order_limit = 0;

SELECT 'LEFT+RIGHT greedy:';
SELECT t1.id, t1.value, t2.id, t2.value, t3.id, t3.value
FROM t1 LEFT JOIN t2 ON t1.id = t2.id
RIGHT JOIN t3 ON t2.id = t3.id ORDER BY ALL
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy';

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
