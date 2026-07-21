DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS tj;

SET enable_analyzer = 1;
SET single_join_prefer_left_table = 0;

CREATE TABLE tj (key2 UInt64, key1 Int64, a UInt64, b UInt64, x UInt64, y UInt64) ENGINE = Join(ALL, RIGHT, key1, key2);
INSERT INTO tj VALUES (2, -2, 20, 200, 2000, 20000), (3, -3, 30, 300, 3000, 30000), (4, -4, 40, 400, 4000, 40000), (5, -5, 50, 500, 5000, 50000), (6, -6, 60, 600, 6000, 60000);

SELECT '--- no name clashes ---';

CREATE TABLE t1 (id2 UInt64, id1 Int64, val UInt64) ENGINE = Memory;
INSERT INTO t1 VALUES (1, -1, 11), (2, -2, 22), (3, -3, 33), (4, -4, 44), (5, -5, 55);

SELECT * FROM t1 ALL RIGHT JOIN tj ON t1.id1 == tj.key1 AND t1.id2 == tj.key2 ORDER BY key1 FORMAT TSVWithNames;
SELECT id1, val, key1, b, x FROM t1 ALL RIGHT JOIN tj ON t1.id1 == tj.key1 AND t1.id2 == tj.key2 ORDER BY key1 FORMAT TSVWithNames;
SELECT t1.id1, t1.val, tj.key1, tj.b, tj.x FROM t1 ALL RIGHT JOIN tj ON t1.id1 == tj.key1 AND t1.id2 == tj.key2 ORDER BY key1 FORMAT TSVWithNames;
SELECT val, b, x FROM t1 ALL RIGHT JOIN tj ON t1.id1 == tj.key1 AND t1.id2 == tj.key2 ORDER BY key1 FORMAT TSVWithNames;
SELECT val FROM t1 ALL RIGHT JOIN tj ON t1.id1 == tj.key1 AND t1.id2 == tj.key2 ORDER BY key1 FORMAT TSVWithNames;
SELECT x FROM t1 ALL RIGHT JOIN tj ON t1.id1 == tj.key1 AND t1.id2 == tj.key2 ORDER BY key1 FORMAT TSVWithNames;

SELECT '--- name clashes ---';

CREATE TABLE t (key2 UInt64, key1 Int64, b UInt64, x UInt64, val UInt64) ENGINE = Memory;
INSERT INTO t VALUES (1, -1, 11, 111, 1111), (2, -2, 22, 222, 2222), (3, -3, 33, 333, 2222), (4, -4, 44, 444, 4444), (5, -5, 55, 555, 5555);

SELECT '-- using --';

SELECT * FROM t ALL RIGHT JOIN tj USING (key1, key2) ORDER BY key1 FORMAT TSVWithNames;
SELECT key1 FROM t ALL RIGHT JOIN tj USING (key1, key2) ORDER BY key1 FORMAT TSVWithNames;
SELECT t.key1, tj.key1 FROM t ALL RIGHT JOIN tj USING (key1, key2) ORDER BY key1 FORMAT TSVWithNames;
SELECT t.key2, tj.key2 FROM t ALL RIGHT JOIN tj USING (key1, key2) ORDER BY key1 FORMAT TSVWithNames;
SELECT t.b, tj.b FROM t ALL RIGHT JOIN tj USING (key1, key2) ORDER BY key1 FORMAT TSVWithNames;
SELECT t.x, tj.b FROM t ALL RIGHT JOIN tj USING (key1, key2) ORDER BY key1 FORMAT TSVWithNames;
SELECT tj.a FROM t ALL RIGHT JOIN tj USING (key1, key2) ORDER BY key1 FORMAT TSVWithNames;
SELECT tj.b FROM t ALL RIGHT JOIN tj USING (key1, key2) ORDER BY key1 FORMAT TSVWithNames;
SELECT tj.x FROM t ALL RIGHT JOIN tj USING (key1, key2) ORDER BY key1 FORMAT TSVWithNames;
SELECT tj.y FROM t ALL RIGHT JOIN tj USING (key1, key2) ORDER BY key1 FORMAT TSVWithNames;
SELECT a FROM t ALL RIGHT JOIN tj USING (key1, key2) ORDER BY key1 FORMAT TSVWithNames;
SELECT b FROM t ALL RIGHT JOIN tj USING (key1, key2) ORDER BY key1 FORMAT TSVWithNames; -- { serverError AMBIGUOUS_IDENTIFIER }
SELECT x FROM t ALL RIGHT JOIN tj USING (key1, key2) ORDER BY key1 FORMAT TSVWithNames; -- { serverError AMBIGUOUS_IDENTIFIER }
SELECT y FROM t ALL RIGHT JOIN tj USING (key1, key2) ORDER BY key1 FORMAT TSVWithNames;
SELECT t.val FROM t ALL RIGHT JOIN tj USING (key1, key2) ORDER BY key1 FORMAT TSVWithNames;
SELECT val FROM t ALL RIGHT JOIN tj USING (key1, key2) ORDER BY key1 FORMAT TSVWithNames;

SELECT '-- on --';

SELECT * FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 ORDER BY t.key1 FORMAT TSVWithNames;
SELECT key1 FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 ORDER BY t.key1 FORMAT TSVWithNames; -- { serverError AMBIGUOUS_IDENTIFIER }
SELECT t.key1, tj.key1 FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 ORDER BY t.key1 FORMAT TSVWithNames;
SELECT t.key2, tj.key2 FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 ORDER BY t.key1 FORMAT TSVWithNames;
SELECT t.b, tj.b FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 ORDER BY t.key1 FORMAT TSVWithNames;
SELECT t.x, tj.b FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 ORDER BY t.key1 FORMAT TSVWithNames;
SELECT tj.a FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 ORDER BY t.key1 FORMAT TSVWithNames;
SELECT tj.b FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 ORDER BY t.key1 FORMAT TSVWithNames;
SELECT tj.x FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 ORDER BY t.key1 FORMAT TSVWithNames;
SELECT tj.y FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 ORDER BY t.key1 FORMAT TSVWithNames;
SELECT a FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 ORDER BY t.key1 FORMAT TSVWithNames;
SELECT b FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 ORDER BY t.key1 FORMAT TSVWithNames; -- { serverError AMBIGUOUS_IDENTIFIER }
SELECT x FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 ORDER BY t.key1 FORMAT TSVWithNames; -- { serverError AMBIGUOUS_IDENTIFIER }
SELECT y FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 ORDER BY t.key1 FORMAT TSVWithNames;
SELECT t.val FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 ORDER BY t.key1 FORMAT TSVWithNames;
SELECT val FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 ORDER BY t.key1 FORMAT TSVWithNames;

SELECT '--- unsupported and illegal conditions ---';

SELECT * FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 + 1 FORMAT TSVWithNames; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
SELECT * FROM t ALL RIGHT JOIN tj ON t.key1 + 1 == tj.key1 AND toUInt64(t.key2 - 1) == tj.key2 ORDER BY t.key1, tj.key2 FORMAT TSVWithNames; -- Ok: expression on the left table

SELECT * FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 AND 1 == 1 ORDER BY ALL SETTINGS query_plan_use_new_logical_join_step = 0 FORMAT TSVWithNames; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
SELECT * FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 AND 1 == 1 ORDER BY ALL SETTINGS query_plan_use_new_logical_join_step = 1, enable_parallel_replicas=0 FORMAT TSVWithNames;
SELECT * FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 AND 1 == 2 FORMAT TSVWithNames; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }

SELECT * FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 AND tj.a == 20 FORMAT TSVWithNames; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
SELECT * FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 AND t.b == 22 ORDER BY t.key1, tj.key2 FORMAT TSVWithNames; -- Ok: t.b from the left table

SELECT * FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 AND 1 != 1 FORMAT TSVWithNames; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
SELECT * FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 AND NULL ORDER BY ALL SETTINGS query_plan_use_new_logical_join_step = 0 FORMAT TSVWithNames; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
SELECT * FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 AND NULL ORDER BY ALL SETTINGS query_plan_use_new_logical_join_step = 1, enable_parallel_replicas=0 FORMAT TSVWithNames;

SELECT * FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 AND 'aaa' FORMAT TSVWithNames; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT * FROM t ALL RIGHT JOIN tj ON 'aaa' FORMAT TSVWithNames; -- { serverError INVALID_JOIN_ON_EXPRESSION }

SELECT * FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 AND 1 ORDER BY ALL SETTINGS query_plan_use_new_logical_join_step = 0 FORMAT TSVWithNames; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
SELECT * FROM t ALL RIGHT JOIN tj ON t.key1 == tj.key1 AND t.key2 == tj.key2 AND 1 ORDER BY ALL SETTINGS query_plan_use_new_logical_join_step = 1, enable_parallel_replicas=0 FORMAT TSVWithNames;
SELECT * FROM t ALL RIGHT JOIN tj ON 0 FORMAT TSVWithNames; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }
SELECT * FROM t ALL RIGHT JOIN tj ON 1 FORMAT TSVWithNames; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS tj;
