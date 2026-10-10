-- A correlated subquery reads a `Merge` table, and one non-empty source table of that `Merge` lacks a column the subquery reads.
-- Planned inside `EXPLAIN`, a subquery in `FROM`, a view or an `IN` subquery, it read freed memory (heap-use-after-free under ASan).

SET allow_correlated_subqueries = 1;

DROP VIEW IF EXISTS v_correlated;
DROP TABLE IF EXISTS t_merge;
DROP TABLE IF EXISTS t_inner;
DROP TABLE IF EXISTS t_narrow;
DROP TABLE IF EXISTS t_outer_empty;

CREATE TABLE t_inner (n Nullable(Int32), c Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_narrow (k UInt16) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_merge (n Nullable(Int32), c Int32) ENGINE = Merge(currentDatabase(), '^t_(inner|narrow)$');
CREATE TABLE t_outer_empty (x Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE VIEW v_correlated AS SELECT x FROM (SELECT 1 AS x) AS o WHERE EXISTS (SELECT 1 FROM t_merge AS i WHERE o.x < i.n AND i.n = i.c);

INSERT INTO t_inner VALUES (1, 2), (2, 2);
INSERT INTO t_narrow VALUES (1), (2), (3);

SELECT 'EXPLAIN, empty outer table';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT x FROM t_outer_empty AS o WHERE EXISTS (SELECT 1 FROM t_merge AS i WHERE o.x < i.n AND i.n = i.c));

SELECT 'EXPLAIN, outer subquery';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT x FROM (SELECT 1 AS x) AS o WHERE EXISTS (SELECT 1 FROM t_merge AS i WHERE o.x < i.n AND i.n = i.c));

SELECT 'subquery in FROM';
SELECT x FROM (SELECT x FROM (SELECT 1 AS x) AS o WHERE EXISTS (SELECT 1 FROM t_merge AS i WHERE o.x < i.n AND i.n = i.c));

SELECT 'scalar subquery inside a subquery in FROM';
SELECT m FROM (SELECT (SELECT max(i.c) FROM t_merge AS i WHERE o.x < i.n AND i.n = i.c) AS m FROM (SELECT 1 AS x) AS o);

SELECT 'IN subquery';
SELECT number FROM numbers(3) WHERE number IN (SELECT x FROM (SELECT 1 AS x) AS o WHERE EXISTS (SELECT 1 FROM t_merge AS i WHERE o.x < i.n AND i.n = i.c));

SELECT 'view';
SELECT x FROM v_correlated;

DROP VIEW v_correlated;
DROP TABLE t_merge;
DROP TABLE t_inner;
DROP TABLE t_narrow;
DROP TABLE t_outer_empty;
