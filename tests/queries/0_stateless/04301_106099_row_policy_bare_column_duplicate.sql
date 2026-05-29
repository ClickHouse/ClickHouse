-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/106099
-- A permissive row policy whose USING expression is a bare physical column
-- reference made any query that reads the same column fail with
-- LOGICAL_ERROR: "Duplicate column name <c> in row policy actions output".
-- The bug was introduced by the position-based unused-column-removal pass
-- in `ReadFromMergeTree` (PR #100586). The duplicate originated in
-- `buildFilterInfo` / `generateFilterActions`, which appended every required
-- input as a filter output without checking whether it was already there;
-- for a bare-column filter the filter result IS the input node, so the same
-- node ended up in the outputs twice.

DROP TABLE IF EXISTS t_106099;
DROP ROW POLICY IF EXISTS pol_106099 ON t_106099;

CREATE TABLE t_106099 (c0 Int32, c1 Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_106099 VALUES (1, 10), (2, 20), (3, 30);

CREATE ROW POLICY pol_106099 ON t_106099 USING c0 TO ALL;

-- Original reproducer from the issue (used to throw LOGICAL_ERROR).
SELECT c0 FROM t_106099 ORDER BY c0;

-- The same bug also affected `SELECT *` and `SELECT count()` once the
-- policy column had to be read by the query.
SELECT * FROM t_106099 ORDER BY c0;
SELECT count() FROM t_106099;

-- Selecting a column that is NOT used by the policy must still work.
SELECT c1 FROM t_106099 ORDER BY c1;

-- The bug was independent of `enable_analyzer` (both 0 and 1 hit it).
SELECT c0 FROM t_106099 ORDER BY c0 SETTINGS enable_analyzer = 0;
SELECT c0 FROM t_106099 ORDER BY c0 SETTINGS enable_analyzer = 1;

-- Wrapping the USING expression (so the filter result is no longer the bare
-- column node) was the documented workaround and must keep working.
DROP ROW POLICY pol_106099 ON t_106099;
CREATE ROW POLICY pol_106099 ON t_106099 USING c0 + 0 TO ALL;
SELECT c0 FROM t_106099 ORDER BY c0;

DROP ROW POLICY pol_106099 ON t_106099;
CREATE ROW POLICY pol_106099 ON t_106099 USING c0 != 0 TO ALL;
SELECT c0 FROM t_106099 ORDER BY c0;

DROP ROW POLICY pol_106099 ON t_106099;
CREATE ROW POLICY pol_106099 ON t_106099 USING materialize(c0) TO ALL;
SELECT c0 FROM t_106099 ORDER BY c0;

DROP ROW POLICY pol_106099 ON t_106099;
DROP TABLE t_106099;
