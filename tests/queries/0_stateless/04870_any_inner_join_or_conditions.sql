-- Tags: no-old-analyzer

-- ANY INNER JOIN returns one row per key of every OR-ed condition, taking the first row of that key
-- from each side, so the result does not depend on which side the hash table is built from: neither on
-- the order the tables are written in, nor on `query_plan_join_swap_table`, nor on the probe order.

DROP TABLE IF EXISTS t_inner_l;
DROP TABLE IF EXISTS t_inner_r;

CREATE TABLE t_inner_l (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_inner_r (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_inner_l SELECT number % 8, number % 3 FROM numbers(12);
INSERT INTO t_inner_r SELECT number % 5, number % 8 FROM numbers(12);

SELECT 'written left to right';
SELECT t_inner_l.a, t_inner_l.b, t_inner_r.a, t_inner_r.b FROM t_inner_l ANY JOIN t_inner_r
ON t_inner_l.b = t_inner_r.b OR t_inner_l.a = t_inner_r.a
ORDER BY ALL SETTINGS max_threads = 1, query_plan_join_swap_table = 'false';

SELECT 'written right to left';
SELECT t_inner_l.a, t_inner_l.b, t_inner_r.a, t_inner_r.b FROM t_inner_r ANY JOIN t_inner_l
ON t_inner_r.b = t_inner_l.b OR t_inner_r.a = t_inner_l.a
ORDER BY ALL SETTINGS max_threads = 1, query_plan_join_swap_table = 'false';

SELECT 'the same, with the tables swapped by the planner';
SELECT t_inner_l.a, t_inner_l.b, t_inner_r.a, t_inner_r.b FROM t_inner_l ANY JOIN t_inner_r
ON t_inner_l.b = t_inner_r.b OR t_inner_l.a = t_inner_r.a
ORDER BY ALL SETTINGS max_threads = 1, query_plan_join_swap_table = 'true';

SELECT 'row count is the same for both swap settings and for several threads';
SELECT
    (SELECT count() FROM (SELECT * FROM t_inner_l ANY JOIN t_inner_r ON t_inner_l.b = t_inner_r.b OR t_inner_l.a = t_inner_r.a) SETTINGS query_plan_join_swap_table = 'false'),
    (SELECT count() FROM (SELECT * FROM t_inner_l ANY JOIN t_inner_r ON t_inner_l.b = t_inner_r.b OR t_inner_l.a = t_inner_r.a) SETTINGS query_plan_join_swap_table = 'true'),
    (SELECT count() FROM (SELECT * FROM t_inner_l ANY JOIN t_inner_r ON t_inner_l.b = t_inner_r.b OR t_inner_l.a = t_inner_r.a) SETTINGS max_threads = 8);

-- A condition over both tables that survives as a residual is executed by another code path, which
-- filters the candidates of a key before claiming it.
SELECT 'with a residual condition: written left to right, written right to left, swapped, 8 threads';
SELECT
    (SELECT count() FROM (SELECT * FROM t_inner_l ANY JOIN t_inner_r ON (t_inner_l.b = t_inner_r.b OR t_inner_l.a = t_inner_r.a) AND t_inner_l.a + t_inner_r.a != -12345) SETTINGS max_threads = 1, query_plan_join_swap_table = 'false'),
    (SELECT count() FROM (SELECT * FROM t_inner_r ANY JOIN t_inner_l ON (t_inner_r.b = t_inner_l.b OR t_inner_r.a = t_inner_l.a) AND t_inner_l.a + t_inner_r.a != -12345) SETTINGS max_threads = 1, query_plan_join_swap_table = 'false'),
    (SELECT count() FROM (SELECT * FROM t_inner_l ANY JOIN t_inner_r ON (t_inner_l.b = t_inner_r.b OR t_inner_l.a = t_inner_r.a) AND t_inner_l.a + t_inner_r.a != -12345) SETTINGS max_threads = 1, query_plan_join_swap_table = 'true'),
    (SELECT count() FROM (SELECT * FROM t_inner_l ANY JOIN t_inner_r ON (t_inner_l.b = t_inner_r.b OR t_inner_l.a = t_inner_r.a) AND t_inner_l.a + t_inner_r.a != -12345) SETTINGS max_threads = 8);

DROP TABLE t_inner_l;
DROP TABLE t_inner_r;

-- One right row is the stored row of a key in both conditions, and the left rows reaching those two
-- keys are read from different parts: the number of result rows must not depend on which of them is
-- probed first.
CREATE TABLE t_inner_l (a Int32, b Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_inner_r (a Int32, b Int32) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES t_inner_l;
INSERT INTO t_inner_r VALUES (1, 5);
INSERT INTO t_inner_l VALUES (1, 9);
INSERT INTO t_inner_l VALUES (1, 5);

SELECT 'both keys of the shared right row are joined, whatever the probe order';
SELECT count() FROM (SELECT * FROM t_inner_l ANY JOIN t_inner_r ON t_inner_l.a = t_inner_r.a OR t_inner_l.b = t_inner_r.b) SETTINGS max_threads = 1;
SELECT count() FROM (SELECT * FROM t_inner_l ANY JOIN t_inner_r ON t_inner_l.a = t_inner_r.a OR t_inner_l.b = t_inner_r.b) SETTINGS max_threads = 8;

DROP TABLE t_inner_l;
DROP TABLE t_inner_r;

-- A right row reachable through a different condition from each of two left rows is joined to both.
CREATE TABLE t_inner_l (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_inner_r (a Int32, b Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_inner_r VALUES (1, 5);
INSERT INTO t_inner_l VALUES (1, 9), (9, 5);

SELECT 'one row per condition that matches';
SELECT t_inner_l.a, t_inner_l.b FROM t_inner_l ANY JOIN t_inner_r ON t_inner_l.a = t_inner_r.a OR t_inner_l.b = t_inner_r.b
ORDER BY ALL SETTINGS max_threads = 1, query_plan_join_swap_table = 'false';

-- A single condition keeps the one-row-per-key behaviour of both sides.
SELECT 'single condition: 2 left rows and 1 right row share the key';
SELECT t_inner_l.a, t_inner_l.b, t_inner_r.a FROM t_inner_l ANY JOIN t_inner_r ON t_inner_l.a = t_inner_r.a
ORDER BY ALL SETTINGS max_threads = 1, query_plan_join_swap_table = 'false';

DROP TABLE t_inner_l;
DROP TABLE t_inner_r;
