-- A set subquery declaring a MATERIALIZED CTE that two OTHER MATERIALIZED CTEs of that subquery read
-- (a UNION ALL whose branches each chain two CTEs, or a single-branch diamond) lost the materialization
-- gate for the shared CTE, so a reader ran before its writer: "Reading from materialized CTE 'c' before
-- its materialization completed - DelayedPortsProcessor gate is missing in the query plan".

SET enable_analyzer = 1;
SET enable_materialized_cte = 1;
SET enable_lightweight_update = 1;
-- Pin: the table must be read with more than one stream, or no reader of the side pipeline is ever
-- scheduled and the bug is invisible.
SET max_threads = 3;

-- Pin: the implicit min-max indices are disabled on the tables below. With an implicit index over `v`
-- the `v IN (<set subquery>)` set is built for skip-index analysis outside the main query plan, so the
-- `EXPLAIN` counts below no longer see the `MaterializingCTE` steps this test is about.

DROP TABLE IF EXISTS t_05230;
DROP TABLE IF EXISTS t_05230_union;
DROP TABLE IF EXISTS t_05230_diamond;

CREATE TABLE t_05230 (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, add_minmax_index_for_numeric_columns = 0;
-- Pin: one part per INSERT, kept separate, for the same reason as max_threads above.
SYSTEM STOP MERGES t_05230;
INSERT INTO t_05230 VALUES (1, 200);
INSERT INTO t_05230 VALUES (2, 1048578);
INSERT INTO t_05230 VALUES (3, 7);
INSERT INTO t_05230 VALUES (4, 0);
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_05230' AND active;

-- Pin: the CTEs are materialized rather than inlined, so the cases below are not vacuous. A CTE read
-- only once is inlined, which is why each one is read twice. The UNION ALL count is 3 because the two
-- branches' identical `c` merge into one CTE while their differing `d` bodies stay two.
SELECT countIf(explain LIKE '%MaterializingCTE (Materializing CTE:%') FROM (
    EXPLAIN SELECT count() FROM t_05230 WHERE v IN (
        WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)),
             d AS MATERIALIZED (SELECT x + 1048577 AS y FROM c)
        SELECT DISTINCT a.y FROM d AS a, d AS b
        UNION ALL
        WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)),
             d AS MATERIALIZED (SELECT x + 200 AS y FROM c)
        SELECT a.y FROM d AS a, d AS b));

SELECT countIf(explain LIKE '%MaterializingCTE (Materializing CTE:%') FROM (
    EXPLAIN SELECT count() FROM t_05230 WHERE v IN (
        WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)),
             d AS MATERIALIZED (SELECT x + 200 AS y FROM c),
             e AS MATERIALIZED (SELECT x + 1048577 AS y FROM c)
        SELECT d1.y FROM d AS d1, d AS d2, e AS e1, e AS e2));

-- Negative control for the pin above: without the MATERIALIZED keyword there are no writer steps at
-- all, so the pin measures materialization rather than always holding.
SELECT countIf(explain LIKE '%MaterializingCTE (Materializing CTE:%') FROM (
    EXPLAIN SELECT count() FROM t_05230 WHERE v IN (
        WITH c AS (SELECT number AS x FROM numbers(3)),
             d AS (SELECT x + 1048577 AS y FROM c)
        SELECT DISTINCT a.y FROM d AS a, d AS b));

-- The reported shape, as a SELECT: the subquery yields {1048577..1048579} + {200..202}, matching v = 200
-- and v = 1048578 only.
SELECT count() FROM t_05230 WHERE v IN (
    WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)),
         d AS MATERIALIZED (SELECT x + 1048577 AS y FROM c)
    SELECT DISTINCT a.y FROM d AS a, d AS b
    UNION ALL
    WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)),
         d AS MATERIALIZED (SELECT x + 200 AS y FROM c)
    SELECT a.y FROM d AS a, d AS b);

-- The same defect without any UNION: two distinct CTEs reading one shared CTE. The subquery yields
-- {200..202}, matching v = 200 only.
SELECT count() FROM t_05230 WHERE v IN (
    WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)),
         d AS MATERIALIZED (SELECT x + 200 AS y FROM c),
         e AS MATERIALIZED (SELECT x + 1048577 AS y FROM c)
    SELECT d1.y FROM d AS d1, d AS d2, e AS e1, e AS e2);

-- The reported shape verbatim: a lightweight UPDATE whose predicate is that set subquery.
CREATE TABLE t_05230_union (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES t_05230_union;
INSERT INTO t_05230_union VALUES (1, 200);
INSERT INTO t_05230_union VALUES (2, 1048578);
INSERT INTO t_05230_union VALUES (3, 7);
INSERT INTO t_05230_union VALUES (4, 0);

UPDATE t_05230_union SET v = 100 WHERE v IN (
    WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)),
         d AS MATERIALIZED (SELECT x + 1048577 AS y FROM c)
    SELECT DISTINCT a.y FROM d AS a, d AS b
    UNION ALL
    WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)),
         d AS MATERIALIZED (SELECT x + 200 AS y FROM c)
    SELECT a.y FROM d AS a, d AS b);

SELECT id, v FROM t_05230_union ORDER BY id;

-- The diamond, as a lightweight UPDATE.
CREATE TABLE t_05230_diamond (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES t_05230_diamond;
INSERT INTO t_05230_diamond VALUES (1, 200);
INSERT INTO t_05230_diamond VALUES (2, 1048578);
INSERT INTO t_05230_diamond VALUES (3, 7);
INSERT INTO t_05230_diamond VALUES (4, 0);

UPDATE t_05230_diamond SET v = 100 WHERE v IN (
    WITH c AS MATERIALIZED (SELECT number AS x FROM numbers(3)),
         d AS MATERIALIZED (SELECT x + 200 AS y FROM c),
         e AS MATERIALIZED (SELECT x + 1048577 AS y FROM c)
    SELECT d1.y FROM d AS d1, d AS d2, e AS e1, e AS e2);

SELECT id, v FROM t_05230_diamond ORDER BY id;

DROP TABLE t_05230;
DROP TABLE t_05230_union;
DROP TABLE t_05230_diamond;
