-- Tags: no-old-analyzer

SET enable_analyzer = 1;
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;
CREATE TABLE t_left (id Int32, val String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_right (id Int32, val String) ENGINE = MergeTree ORDER BY id;

SELECT '-- ALL INNER JOIN, duplicated right key: each left row is emitted once per right match';
INSERT INTO t_left VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO t_right VALUES (1, 'x'), (1, 'y'), (2, 'z');

SELECT id, val FROM t_left ALL INNER JOIN t_right ON t_left.id = t_right.id
ORDER BY id, val SETTINGS query_plan_convert_join_to_in = 0;

SELECT id, val FROM t_left ALL INNER JOIN t_right ON t_left.id = t_right.id
ORDER BY id, val SETTINGS query_plan_convert_join_to_in = 1;

SELECT '-- ANY INNER JOIN, duplicated left key, unique right key: left side is deduplicated';
TRUNCATE TABLE t_left;
TRUNCATE TABLE t_right;
INSERT INTO t_left VALUES (1, 'a'), (1, 'a2'), (1, 'a3'), (2, 'b'), (2, 'b2');
INSERT INTO t_right VALUES (1, 'x'), (2, 'z');

SELECT id, val FROM t_left ANY INNER JOIN t_right ON t_left.id = t_right.id
ORDER BY id, val SETTINGS query_plan_convert_join_to_in = 0;

SELECT id, val FROM t_left ANY INNER JOIN t_right ON t_left.id = t_right.id
ORDER BY id, val SETTINGS query_plan_convert_join_to_in = 1;

SELECT '-- SEMI LEFT JOIN is convertible: each matching left row is emitted exactly once';
TRUNCATE TABLE t_left;
TRUNCATE TABLE t_right;
INSERT INTO t_left VALUES (1, 'a'), (1, 'a2'), (2, 'b'), (9, 'nomatch');
INSERT INTO t_right VALUES (1, 'x'), (1, 'y'), (2, 'z');

SELECT val FROM t_left SEMI LEFT JOIN t_right ON t_left.id = t_right.id
ORDER BY val SETTINGS query_plan_convert_join_to_in = 0;

SELECT val FROM t_left SEMI LEFT JOIN t_right ON t_left.id = t_right.id
ORDER BY val SETTINGS query_plan_convert_join_to_in = 1;

SELECT '-- and it does convert';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT val FROM t_left SEMI LEFT JOIN t_right ON t_left.id = t_right.id
    SETTINGS query_plan_convert_join_to_in = 1
) WHERE explain ILIKE '%CreatingSets%';

SELECT '-- while ALL and ANY do not';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT t_left.id FROM t_left ALL INNER JOIN t_right ON t_left.id = t_right.id
    SETTINGS query_plan_convert_join_to_in = 1
) WHERE explain ILIKE '%CreatingSets%';

SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT t_left.id FROM t_left ANY INNER JOIN t_right ON t_left.id = t_right.id
    SETTINGS query_plan_convert_join_to_in = 1
) WHERE explain ILIKE '%CreatingSets%';

SELECT '-- a Join engine table keeps its declared strictness check';
DROP TABLE IF EXISTS t_join_any;
CREATE TABLE t_join_any (id Int32, rval String) ENGINE = Join(ANY, LEFT, id);
INSERT INTO t_join_any VALUES (1, 'x'), (2, 'z');

SELECT count() FROM (SELECT val FROM t_left SEMI LEFT JOIN t_join_any ON t_left.id = t_join_any.id)
SETTINGS query_plan_convert_join_to_in = 0; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }

SELECT count() FROM (SELECT val FROM t_left SEMI LEFT JOIN t_join_any ON t_left.id = t_join_any.id)
SETTINGS query_plan_convert_join_to_in = 1; -- { serverError INCOMPATIBLE_TYPE_OF_JOIN }

DROP TABLE t_join_any;
DROP TABLE t_left;
DROP TABLE t_right;
