-- Tags: no-old-analyzer

SET enable_analyzer = 1;
SET join_algorithm = 'hash';
-- The conversion declines while a join or transfer limit is active, and the test profile sets all four.
SET max_rows_in_join = 0, max_bytes_in_join = 0, max_rows_to_transfer = 0, max_bytes_to_transfer = 0;

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

SELECT '-- legacy any_join_distinct_right_table_keys rewrites ANY INNER to SEMI LEFT';
SELECT val FROM t_left ANY INNER JOIN t_right ON t_left.id = t_right.id
ORDER BY val SETTINGS any_join_distinct_right_table_keys = 1, query_plan_convert_join_to_in = 0;

SELECT val FROM t_left ANY INNER JOIN t_right ON t_left.id = t_right.id
ORDER BY val SETTINGS any_join_distinct_right_table_keys = 1, query_plan_convert_join_to_in = 1;

SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT val FROM t_left ANY INNER JOIN t_right ON t_left.id = t_right.id
    SETTINGS any_join_distinct_right_table_keys = 1, query_plan_convert_join_to_in = 1
) WHERE explain ILIKE '%CreatingSets%';

SELECT '-- a JOIN row limit is enforced whether or not the join is converted';
SELECT val FROM t_left SEMI LEFT JOIN t_right ON t_left.id = t_right.id
ORDER BY val SETTINGS query_plan_convert_join_to_in = 0, max_rows_in_join = 1; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT val FROM t_left SEMI LEFT JOIN t_right ON t_left.id = t_right.id
ORDER BY val SETTINGS query_plan_convert_join_to_in = 1, max_rows_in_join = 1; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

SELECT '-- and a transfer row limit does not silently truncate the result';
SELECT val FROM t_left SEMI LEFT JOIN t_right ON t_left.id = t_right.id
ORDER BY val SETTINGS query_plan_convert_join_to_in = 0, max_rows_to_transfer = 1, transfer_overflow_mode = 'break';

SELECT val FROM t_left SEMI LEFT JOIN t_right ON t_left.id = t_right.id
ORDER BY val SETTINGS query_plan_convert_join_to_in = 1, max_rows_to_transfer = 1, transfer_overflow_mode = 'break';

SELECT '-- a query that sets either limit is not converted';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT val FROM t_left SEMI LEFT JOIN t_right ON t_left.id = t_right.id
    SETTINGS query_plan_convert_join_to_in = 1, max_rows_in_join = 1
) WHERE explain ILIKE '%CreatingSets%';

SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT val FROM t_left SEMI LEFT JOIN t_right ON t_left.id = t_right.id
    SETTINGS query_plan_convert_join_to_in = 1, max_rows_to_transfer = 1, transfer_overflow_mode = 'break'
) WHERE explain ILIKE '%CreatingSets%';

SELECT '-- equal numeric limits are not equal behaviour: the two regimes bound different quantities';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT val FROM t_left SEMI LEFT JOIN t_right ON t_left.id = t_right.id
    SETTINGS query_plan_convert_join_to_in = 1, max_rows_in_join = 5, max_rows_to_transfer = 5
) WHERE explain ILIKE '%CreatingSets%';

SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT val FROM t_left SEMI LEFT JOIN t_right ON t_left.id = t_right.id
    SETTINGS query_plan_convert_join_to_in = 1, max_bytes_in_join = 1000000, max_bytes_to_transfer = 1000000
) WHERE explain ILIKE '%CreatingSets%';

SELECT '-- a differing overflow mode alone does not block the conversion';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT val FROM t_left SEMI LEFT JOIN t_right ON t_left.id = t_right.id
    SETTINGS query_plan_convert_join_to_in = 1, join_overflow_mode = 'break'
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

SELECT '-- a key with dynamic structure is not converted: the IN function rejects such an argument';
DROP TABLE IF EXISTS t_dyn_left;
DROP TABLE IF EXISTS t_dyn_right;
CREATE TABLE t_dyn_left (id Dynamic, val String) ENGINE = Memory;
CREATE TABLE t_dyn_right (id Dynamic) ENGINE = Memory;
INSERT INTO t_dyn_left VALUES (1, 'a'), (2, 'b'), (9, 'nomatch');
INSERT INTO t_dyn_right VALUES (1), (2);

SELECT val FROM t_dyn_left SEMI LEFT JOIN t_dyn_right ON t_dyn_left.id = t_dyn_right.id
ORDER BY val SETTINGS allow_dynamic_type_in_join_keys = 1, query_plan_convert_join_to_in = 0;

SELECT val FROM t_dyn_left SEMI LEFT JOIN t_dyn_right ON t_dyn_left.id = t_dyn_right.id
ORDER BY val SETTINGS allow_dynamic_type_in_join_keys = 1, query_plan_convert_join_to_in = 1;

SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT val FROM t_dyn_left SEMI LEFT JOIN t_dyn_right ON t_dyn_left.id = t_dyn_right.id
    SETTINGS allow_dynamic_type_in_join_keys = 1, query_plan_convert_join_to_in = 1
) WHERE explain ILIKE '%CreatingSets%';

SELECT '-- including a Dynamic nested inside a composite key type';
DROP TABLE IF EXISTS t_arr_left;
DROP TABLE IF EXISTS t_arr_right;
CREATE TABLE t_arr_left (id Array(Dynamic), val String) ENGINE = Memory;
CREATE TABLE t_arr_right (id Array(Dynamic)) ENGINE = Memory;
INSERT INTO t_arr_left VALUES ([1], 'a'), ([2], 'b'), ([9], 'nomatch');
INSERT INTO t_arr_right VALUES ([1]), ([2]);

SELECT val FROM t_arr_left SEMI LEFT JOIN t_arr_right ON t_arr_left.id = t_arr_right.id
ORDER BY val SETTINGS allow_dynamic_type_in_join_keys = 1, query_plan_convert_join_to_in = 0;

SELECT val FROM t_arr_left SEMI LEFT JOIN t_arr_right ON t_arr_left.id = t_arr_right.id
ORDER BY val SETTINGS allow_dynamic_type_in_join_keys = 1, query_plan_convert_join_to_in = 1;

SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT val FROM t_arr_left SEMI LEFT JOIN t_arr_right ON t_arr_left.id = t_arr_right.id
    SETTINGS allow_dynamic_type_in_join_keys = 1, query_plan_convert_join_to_in = 1
) WHERE explain ILIKE '%CreatingSets%';

DROP TABLE t_arr_left;
DROP TABLE t_arr_right;
DROP TABLE t_dyn_left;
DROP TABLE t_dyn_right;
DROP TABLE t_left;
DROP TABLE t_right;
