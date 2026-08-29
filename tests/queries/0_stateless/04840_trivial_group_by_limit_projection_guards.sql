-- The trivial `GROUP BY ... LIMIT` optimization (`optimize_trivial_group_by_limit_query`)
-- must not fire when something between the aggregation and the LIMIT consumes or filters
-- the groups: cutting the aggregation at `LIMIT + OFFSET` keys then changes the result
-- instead of merely picking an unspecified subset of the groups. Each of the cases below
-- returned wrong results (fewer rows or wrong values) before the guards were added, as
-- pinned by the expected outputs.
--
-- The queries run at the top level of an `INSERT ... SELECT` because the pass fires only
-- on the top-level query. The result of the problematic query is captured into a table
-- because its correct output rows are an unspecified subset of the groups; the assertions
-- are on deterministic aggregates of it.

SET enable_analyzer = 1;
SET optimize_trivial_group_by_limit_query = 1;
SET max_threads = 16;
SET max_block_size = 100;

DROP TABLE IF EXISTS t_04840;
CREATE TABLE t_04840 (v UInt64) ENGINE = Memory;

-- DISTINCT collapses the projected groups: 100000 groups yield 4 distinct values of
-- `intDiv(k, 25000)`, and LIMIT 3 must return 3 of them. With aggregation cut at 3 keys
-- the distinct set shrank to 1 value.
INSERT INTO t_04840 SELECT DISTINCT intDiv(k, 25000) FROM (SELECT number AS k FROM numbers_mt(100000)) GROUP BY k LIMIT 3;
SELECT count() FROM t_04840;
TRUNCATE TABLE t_04840;

-- A window function in the projection is evaluated over all groups: `count() OVER ()`
-- must see all 100000 of them. With aggregation cut near the LIMIT it saw only the kept
-- groups (values like 100).
INSERT INTO t_04840 SELECT count() OVER () FROM (SELECT number AS k FROM numbers_mt(100000)) GROUP BY k LIMIT 5;
SELECT count(), min(v), max(v) FROM t_04840;
TRUNCATE TABLE t_04840;

-- `arrayJoin` in the projection can drop rows (empty arrays): only 100 of the 100000
-- groups produce a row, so LIMIT 10 must still find 10 rows. With aggregation cut near
-- the LIMIT almost all kept groups produced nothing (1 row came out).
INSERT INTO t_04840 SELECT arrayJoin(if(k % 1000 = 0, [k], [])) FROM (SELECT number AS k FROM numbers_mt(100000)) GROUP BY k LIMIT 10;
SELECT count() FROM t_04840;
TRUNCATE TABLE t_04840;

-- QUALIFY filters the groups after the aggregation: 1000 groups pass the filter, so
-- LIMIT 10 must find 10 rows, and the window function must see all 50000 groups of its
-- partition. With aggregation cut near the LIMIT none of the kept groups qualified
-- (0 rows came out). A QUALIFY on plain group keys alone is pushed down below the
-- aggregation and does not need the guard; the window function keeps it in place.
INSERT INTO t_04840 SELECT count() OVER (PARTITION BY k % 2) FROM (SELECT number AS k FROM numbers_mt(100000)) GROUP BY k QUALIFY k >= 99000 LIMIT 10;
SELECT count(), min(v), max(v) FROM t_04840;
TRUNCATE TABLE t_04840;

DROP TABLE t_04840;
