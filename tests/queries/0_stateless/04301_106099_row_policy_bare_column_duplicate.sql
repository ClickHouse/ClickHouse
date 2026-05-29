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
--
-- The data set includes a row with `c0 = 0` so that the test fails not only
-- when the LOGICAL_ERROR reappears, but also when the policy stops being
-- enforced (e.g. if a future change accidentally bypasses the bare-column
-- filter): the falsy `c0 = 0` row must be excluded from every output.

DROP TABLE IF EXISTS t_106099;
DROP ROW POLICY IF EXISTS pol_106099 ON t_106099;

CREATE TABLE t_106099 (c0 Int32, c1 Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_106099 VALUES (0, 0), (1, 10), (2, 20), (3, 30);

CREATE ROW POLICY pol_106099 ON t_106099 USING c0 TO ALL;

-- Original reproducer from the issue (used to throw LOGICAL_ERROR). The `0`
-- row must be filtered out -- if the policy stopped being applied we would
-- see four rows instead of three.
SELECT c0 FROM t_106099 ORDER BY c0;
SELECT count() FROM t_106099;
SELECT countIf(c0 = 0) FROM t_106099;  -- must be 0: the policy filters falsy rows

-- The same bug also affected `SELECT *` once the policy column had to be
-- read by the query.
SELECT * FROM t_106099 ORDER BY c0;

-- Selecting a column that is NOT used by the policy still gets filtered.
SELECT c1 FROM t_106099 ORDER BY c1;

-- The bug was independent of `enable_analyzer` (both 0 and 1 hit it). The
-- filter must apply under both code paths.
SELECT c0 FROM t_106099 ORDER BY c0 SETTINGS enable_analyzer = 0;
SELECT c0 FROM t_106099 ORDER BY c0 SETTINGS enable_analyzer = 1;
SELECT count() FROM t_106099 SETTINGS enable_analyzer = 0;
SELECT count() FROM t_106099 SETTINGS enable_analyzer = 1;

-- Wrapping the USING expression (so the filter result is no longer the bare
-- column node) was the documented workaround and must keep working. The `0`
-- row is still falsy under each wrapping and must be filtered out.
DROP ROW POLICY pol_106099 ON t_106099;
CREATE ROW POLICY pol_106099 ON t_106099 USING c0 + 0 TO ALL;
SELECT c0 FROM t_106099 ORDER BY c0;
SELECT count() FROM t_106099;

DROP ROW POLICY pol_106099 ON t_106099;
CREATE ROW POLICY pol_106099 ON t_106099 USING c0 != 0 TO ALL;
SELECT c0 FROM t_106099 ORDER BY c0;
SELECT count() FROM t_106099;

DROP ROW POLICY pol_106099 ON t_106099;
CREATE ROW POLICY pol_106099 ON t_106099 USING materialize(c0) TO ALL;
SELECT c0 FROM t_106099 ORDER BY c0;
SELECT count() FROM t_106099;

DROP ROW POLICY pol_106099 ON t_106099;
DROP TABLE t_106099;
