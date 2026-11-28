-- A materialized CTE named identically to a base table it reads inside its own
-- body must bind that self-reference to the base table (recursive CTEs are not
-- supported here), exactly like a non-materialized CTE. When the body is a set
-- operation the name-resolution guard used to check the wrong node, so a later
-- branch self-referenced the still-unmaterialized CTE and produced a read with
-- no `DelayedPortsProcessor` gate (`LOGICAL_ERROR`). STID 2467-2c2d.

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

DROP TABLE IF EXISTS test1 SYNC;
CREATE TABLE test1 (i Int64, j Int64) ENGINE = Memory;
INSERT INTO test1 SELECT number, number FROM numbers(100);

-- Minimal reproducer: threw `DelayedPortsProcessor gate is missing`.
WITH test1 AS MATERIALIZED ((SELECT i FROM test1) EXCEPT ALL (SELECT i FROM test1))
SELECT 4 AS i WHERE i IN (test1);

WITH test1 AS MATERIALIZED ((SELECT i FROM test1) UNION ALL (SELECT i FROM test1))
SELECT 4 AS i WHERE i IN (test1);

WITH test1 AS MATERIALIZED ((SELECT i FROM test1) INTERSECT (SELECT i FROM test1))
SELECT 4 AS i WHERE i IN (test1);

-- Correctness: both branches must read the base table, so a materialized CTE
-- gives the same result as the non-materialized one (200 = 100 + 100).
SELECT 'union count';
WITH test1 AS MATERIALIZED ((SELECT i FROM test1) UNION ALL (SELECT i FROM test1))
SELECT count() FROM test1
SETTINGS enable_materialized_cte = 1;
WITH test1 AS ((SELECT i FROM test1) UNION ALL (SELECT i FROM test1))
SELECT count() FROM test1
SETTINGS enable_materialized_cte = 0;

-- Two consumers of the same materialized CTE: the second reference reuses the
-- storage created by the first, and that path resolves its own copy of the body.
-- Without the guard there, the body's `FROM test1` bound back to the CTE and
-- recursed until `TOO_DEEP_SUBQUERIES`. A single consumer cannot catch this.
-- The counts below match the non-materialized CTE, so first assert the CTE really
-- is materialized once - otherwise they would also pass if it were inlined.
SELECT 'is materialized';
SELECT count() FROM (
    EXPLAIN
    WITH test1 AS MATERIALIZED ((SELECT i FROM test1) UNION ALL (SELECT i FROM test1))
    SELECT count() FROM test1, test1 AS t2
) WHERE explain ILIKE '%MaterializingCTE (Materializing CTE: test1)%'
SETTINGS enable_materialized_cte = 1;

SELECT 'two consumers';
WITH test1 AS MATERIALIZED ((SELECT i FROM test1) UNION ALL (SELECT i FROM test1))
SELECT count() FROM test1, test1 AS t2
SETTINGS enable_materialized_cte = 1;
WITH test1 AS ((SELECT i FROM test1) UNION ALL (SELECT i FROM test1))
SELECT count() FROM test1, test1 AS t2
SETTINGS enable_materialized_cte = 0;

-- A set operation is not required to reach that path - two consumers suffice.
SELECT 'two consumers, no set operation';
WITH test1 AS MATERIALIZED (SELECT i FROM test1)
SELECT count() FROM test1, test1 AS t2
SETTINGS enable_materialized_cte = 1;
WITH test1 AS (SELECT i FROM test1)
SELECT count() FROM test1, test1 AS t2
SETTINGS enable_materialized_cte = 0;

-- Original fuzzer shape (STID 2467-2c2d): two-column DISTINCT + EXCEPT ALL.
WITH test1 AS MATERIALIZED
(
    (SELECT DISTINCT i + 1, j + 1 FROM test1)
    EXCEPT ALL
    (SELECT DISTINCT i + 1, j + 1 FROM test1)
)
SELECT toInt64(4) AS i, toInt64(5) AS j FROM numbers(3) WHERE (i, j) IN (test1);

DROP TABLE test1;
