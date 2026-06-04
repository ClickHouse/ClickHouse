-- Tags: no-random-merge-tree-settings

SET convert_query_to_cnf = 1;
SET optimize_using_constraints = 1;
SET optimize_move_to_prewhere = 1;
SET optimize_substitute_columns = 1;
SET optimize_append_index = 1;
SET optimize_trivial_insert_select = 1;

DROP TABLE IF EXISTS column_swap_test_test;

CREATE TABLE column_swap_test_test (i Int64, a String, b UInt64, CONSTRAINT c1 ASSUME b = cityHash64(a))
ENGINE = MergeTree() ORDER BY i
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO column_swap_test_test VALUES (1, 'cat', 1), (2, 'dog', 2);
INSERT INTO column_swap_test_test SELECT number AS i, format('test {} kek {}', toString(number), toString(number + 10))  AS a, 1 AS b FROM system.numbers LIMIT 1000000;

EXPLAIN SYNTAX SELECT cityHash64(a) + 10, b + 3 FROM column_swap_test_test WHERE cityHash64(a) = 1;
EXPLAIN QUERY TREE SELECT cityHash64(a) + 10, b + 3 FROM column_swap_test_test WHERE cityHash64(a) = 1 SETTINGS enable_analyzer = 1;
EXPLAIN SYNTAX SELECT cityHash64(a) + 10, b + 3 FROM column_swap_test_test PREWHERE cityHash64(a) = 1;
EXPLAIN QUERY TREE SELECT cityHash64(a) + 10, b + 3 FROM column_swap_test_test PREWHERE cityHash64(a) = 1 SETTINGS enable_analyzer = 1;
EXPLAIN SYNTAX SELECT cityHash64(a) + 10, b + 3 FROM column_swap_test_test WHERE cityHash64(a) = 0;
EXPLAIN QUERY TREE SELECT cityHash64(a) + 10, b + 3 FROM column_swap_test_test WHERE cityHash64(a) = 0 SETTINGS enable_analyzer = 1;
EXPLAIN SYNTAX SELECT cityHash64(a) + 10, b + 3 FROM column_swap_test_test WHERE b = 0;
EXPLAIN QUERY TREE SELECT cityHash64(a) + 10, b + 3 FROM column_swap_test_test WHERE b = 0 SETTINGS enable_analyzer = 1;
EXPLAIN SYNTAX SELECT cityHash64(a) + 10, b + 3 FROM column_swap_test_test WHERE b = 1;
EXPLAIN QUERY TREE SELECT cityHash64(a) + 10, b + 3 FROM column_swap_test_test WHERE b = 1 SETTINGS enable_analyzer = 1;

EXPLAIN SYNTAX SELECT cityHash64(a) + 10 FROM column_swap_test_test WHERE cityHash64(a) = 0;
EXPLAIN QUERY TREE SELECT cityHash64(a) + 10 FROM column_swap_test_test WHERE cityHash64(a) = 0 SETTINGS enable_analyzer = 1;
EXPLAIN SYNTAX SELECT cityHash64(a) + 10, a FROM column_swap_test_test WHERE cityHash64(a) = 0;
EXPLAIN QUERY TREE SELECT cityHash64(a) + 10, a FROM column_swap_test_test WHERE cityHash64(a) = 0 SETTINGS enable_analyzer = 1;
EXPLAIN SYNTAX SELECT b + 10, a FROM column_swap_test_test WHERE b = 0;
EXPLAIN QUERY TREE SELECT b + 10, a FROM column_swap_test_test WHERE b = 0 SETTINGS enable_analyzer = 1;

DROP TABLE column_swap_test_test;

CREATE TABLE column_swap_test_test (i Int64, a String, b String, CONSTRAINT c1 ASSUME a = substring(reverse(b), 1, 1))
ENGINE = MergeTree() ORDER BY i
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO column_swap_test_test SELECT number AS i, toString(number) AS a, format('test {} kek {}', toString(number), toString(number + 10)) b FROM system.numbers LIMIT 1000000;

EXPLAIN SYNTAX SELECT substring(reverse(b), 1, 1), a FROM column_swap_test_test WHERE a = 'c';
EXPLAIN QUERY TREE SELECT substring(reverse(b), 1, 1), a FROM column_swap_test_test WHERE a = 'c' SETTINGS enable_analyzer = 1;
EXPLAIN SYNTAX SELECT substring(reverse(b), 1, 1), a FROM column_swap_test_test WHERE substring(reverse(b), 1, 1) = 'c';
EXPLAIN QUERY TREE SELECT substring(reverse(b), 1, 1), a FROM column_swap_test_test WHERE substring(reverse(b), 1, 1) = 'c' SETTINGS enable_analyzer = 1;
EXPLAIN SYNTAX SELECT substring(reverse(b), 1, 1) AS t1, a AS t2 FROM column_swap_test_test WHERE substring(reverse(b), 1, 1) = 'c';
EXPLAIN QUERY TREE SELECT substring(reverse(b), 1, 1) AS t1, a AS t2 FROM column_swap_test_test WHERE substring(reverse(b), 1, 1) = 'c' SETTINGS enable_analyzer = 1;
EXPLAIN SYNTAX SELECT substring(reverse(b), 1, 1) FROM column_swap_test_test WHERE substring(reverse(b), 1, 1) = 'c';
EXPLAIN QUERY TREE SELECT substring(reverse(b), 1, 1) FROM column_swap_test_test WHERE substring(reverse(b), 1, 1) = 'c' SETTINGS enable_analyzer = 1;

DROP TABLE column_swap_test_test;

DROP TABLE IF EXISTS t_bad_constraint;

CREATE TABLE t_bad_constraint(a UInt32, s String, CONSTRAINT c1 ASSUME a = toUInt32(s)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_bad_constraint SELECT number, randomPrintableASCII(100) FROM numbers(10000);

EXPLAIN SYNTAX SELECT a FROM t_bad_constraint;
EXPLAIN QUERY TREE SELECT a FROM t_bad_constraint SETTINGS enable_analyzer = 1;

DROP TABLE t_bad_constraint;
