-- A `Merge` table optimizes every child plan on its own, but all children end up in one
-- pipeline below the outer query's `MaterializingCTEsStep`. If a child plan is allowed to
-- claim a materialized CTE that the outer query references too, the CTE's writer lands in
-- that one child's plan while sibling children read the CTE's storage with no
-- `DelayedPortsProcessor` between them, and `ReadFromMemoryStorageStep` throws
-- `Reading from materialized CTE '...' before its materialization completed`.
--
-- The shape below is order-sensitive: the `View` child is planned first (children are
-- visited in table-name order) and claims the CTE, and the `Distributed` child that
-- follows then builds the `IN` set in place while the CTE is still unbuilt.

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

DROP TABLE IF EXISTS t_04811 SYNC;
DROP TABLE IF EXISTS t_04811_dist SYNC;
DROP VIEW IF EXISTS t_04811_const SYNC;

CREATE TABLE t_04811 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_04811 SELECT number FROM numbers(10);

CREATE TABLE t_04811_dist AS t_04811 ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_04811);
CREATE VIEW t_04811_const AS SELECT toUInt64(1) AS x;

-- 1. The original repro: `View` child before `Distributed` child. No row can satisfy both
--    `x IN (t)` and `x NOT IN (t)`.
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(2))
SELECT count() FROM merge(currentDatabase(), '^t_04811_(const|dist)$')
WHERE (x IN (t)) AND (x NOT IN (t));

-- 2. The same through explicit `IN` subqueries rather than the table shorthand.
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(2))
SELECT count() FROM merge(currentDatabase(), '^t_04811_(const|dist)$')
WHERE (x IN (SELECT c FROM t)) AND (x NOT IN (SELECT c FROM t));

-- 3. A satisfiable predicate, to pin the data and not only the absence of the exception:
--    0 and 1 from `t_04811` plus 1 from the view.
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(2))
SELECT count() FROM merge(currentDatabase(), '^t_04811_(const|dist)$')
WHERE (x IN (t)) AND (x IN (SELECT c FROM t));

-- 4. `EXPLAIN` alone used to throw as well: the in-place set build runs during plan
--    optimization, before any of the query's own pipeline exists.
SELECT count() > 0 FROM (
    EXPLAIN
    WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(2))
    SELECT count() FROM merge(currentDatabase(), '^t_04811_(const|dist)$')
    WHERE (x IN (t)) AND (x NOT IN (t))
);

-- 5. Reverse child order (`Distributed` first, `View` second) - this one always worked and
--    must keep working.
DROP TABLE IF EXISTS a_04811_dist SYNC;
CREATE TABLE a_04811_dist AS t_04811 ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_04811);

WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(2))
SELECT count() FROM merge(currentDatabase(), '^(a_04811_dist|t_04811_const)$')
WHERE (x IN (t)) AND (x NOT IN (t));

-- 6. A materialized CTE defined *inside* a `View` that a `Merge` table reads is owned by
--    that child alone (the outer query cannot see it), so the child must keep materializing
--    it itself. Two references keep it from being inlined: 3 + 3 rows, plus 1 from the
--    constant view.
DROP VIEW IF EXISTS t_04811_view_cte SYNC;
CREATE VIEW t_04811_view_cte AS
    WITH inner_cte AS MATERIALIZED (SELECT number AS x FROM numbers(3))
    SELECT x FROM inner_cte UNION ALL SELECT x FROM inner_cte;

SELECT count() FROM merge(currentDatabase(), '^t_04811_(view_cte|const)$');

DROP VIEW t_04811_view_cte;
DROP VIEW t_04811_const;
DROP TABLE a_04811_dist;
DROP TABLE t_04811_dist;
DROP TABLE t_04811;
