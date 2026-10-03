-- Several result columns backed by a single column, combined with ORDER BY ... WITH FILL ... INTERPOLATE.
-- With `enable_analyzer = 0` the filling transform works on the block before the final projection, where
-- such result columns are one column, so INTERPOLATE targets naming them collapse into a single target.
-- https://github.com/ClickHouse/ClickHouse/issues/111927

DROP TABLE IF EXISTS t_interpolate_aliases;
CREATE TABLE t_interpolate_aliases (n Float32, x UInt64) ENGINE = Memory;
INSERT INTO t_interpolate_aliases VALUES (0, 0), (1, 10);

-- The collapsed targets are interpolated in the same way, so both analyzers agree.

SELECT 'empty INTERPOLATE';
SELECT n, x AS a, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE () SETTINGS enable_analyzer = 0;
SELECT n, x AS a, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE () SETTINGS enable_analyzer = 1;

SELECT 'empty INTERPOLATE, one column selected without an alias';
SELECT n, x, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE () SETTINGS enable_analyzer = 0;
SELECT n, x, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE () SETTINGS enable_analyzer = 1;

SELECT 'empty INTERPOLATE, aliases of one expression';
SELECT n, x + 1 AS a, x + 1 AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE () SETTINGS enable_analyzer = 0;
SELECT n, x + 1 AS a, x + 1 AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE () SETTINGS enable_analyzer = 1;

SELECT 'both targets interpolated by themselves';
SELECT n, x AS a, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS a, b AS b) SETTINGS enable_analyzer = 0;
SELECT n, x AS a, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS a, b AS b) SETTINGS enable_analyzer = 1;

SELECT 'both targets interpolated by the same constant';
SELECT n, x AS a, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS 1, b AS 1) SETTINGS enable_analyzer = 0;
SELECT n, x AS a, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS 1, b AS 1) SETTINGS enable_analyzer = 1;

SELECT 'both targets interpolated by the same expression over both aliases';
SELECT n, x AS a, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS a + b, b AS a + b) SETTINGS enable_analyzer = 0;
SELECT n, x AS a, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS a + b, b AS a + b) SETTINGS enable_analyzer = 1;

SELECT 'targets interpolated by each other';
SELECT n, x AS a, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS b, b AS a) SETTINGS enable_analyzer = 0;
SELECT n, x AS a, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS b, b AS a) SETTINGS enable_analyzer = 1;

SELECT 'expression over an alias and the column it is backed by';
SELECT n, x AS a FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS a + x) SETTINGS enable_analyzer = 0;
SELECT n, x AS a FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS a + x) SETTINGS enable_analyzer = 1;

-- The collapsed targets would have to hold different values, which only the analyzer can do.

SELECT 'different constants';
SELECT n, x AS a, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS 1, b AS 2) SETTINGS enable_analyzer = 0; -- { serverError NOT_IMPLEMENTED }
SELECT n, x AS a, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS 1, b AS 2) SETTINGS enable_analyzer = 1;

SELECT 'only the first alias is a target';
SELECT n, x AS a, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS a + 1) SETTINGS enable_analyzer = 0; -- { serverError NOT_IMPLEMENTED }
SELECT n, x AS a, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS a + 1) SETTINGS enable_analyzer = 1;

SELECT 'only the second alias is a target';
SELECT n, x AS a, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (b AS 7) SETTINGS enable_analyzer = 0; -- { serverError NOT_IMPLEMENTED }
SELECT n, x AS a, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (b AS 7) SETTINGS enable_analyzer = 1;

SELECT 'a target using a sibling alias that is not a target itself';
SELECT n, x AS a, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS a + b) SETTINGS enable_analyzer = 0; -- { serverError NOT_IMPLEMENTED }
SELECT n, x AS a, x AS b FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS a + b) SETTINGS enable_analyzer = 1;

SELECT 'the column the target is backed by is also selected';
SELECT n, x AS a, x FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS 1) SETTINGS enable_analyzer = 0; -- { serverError NOT_IMPLEMENTED }
SELECT n, x AS a, x FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS 1) SETTINGS enable_analyzer = 1;

-- An INTERPOLATE target backed by an ORDER BY ... WITH FILL column keeps its own, more specific error.
SELECT n AS m, n AS k, x FROM t_interpolate_aliases ORDER BY n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (m AS 5) SETTINGS enable_analyzer = 0; -- { serverError INVALID_WITH_FILL_EXPRESSION }

DROP TABLE t_interpolate_aliases;

-- The same, with a sorting prefix before the filled column.

DROP TABLE IF EXISTS t_interpolate_aliases_prefix;
CREATE TABLE t_interpolate_aliases_prefix (g UInt8, n Float32, x UInt64) ENGINE = Memory;
INSERT INTO t_interpolate_aliases_prefix VALUES (0, 0, 0), (0, 1, 20), (1, 0, 10), (1, 1, 30);

SELECT 'sorting prefix, empty INTERPOLATE';
SELECT g, n, x AS a, x AS b FROM t_interpolate_aliases_prefix ORDER BY g, n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE () SETTINGS enable_analyzer = 0, use_with_fill_by_sorting_prefix = 1;
SELECT g, n, x AS a, x AS b FROM t_interpolate_aliases_prefix ORDER BY g, n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE () SETTINGS enable_analyzer = 1, use_with_fill_by_sorting_prefix = 1;

SELECT 'sorting prefix, both targets interpolated by the same expression';
SELECT g, n, x AS a, x AS b FROM t_interpolate_aliases_prefix ORDER BY g, n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS a + 1, b AS a + 1) SETTINGS enable_analyzer = 0, use_with_fill_by_sorting_prefix = 1;
SELECT g, n, x AS a, x AS b FROM t_interpolate_aliases_prefix ORDER BY g, n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS a + 1, b AS a + 1) SETTINGS enable_analyzer = 1, use_with_fill_by_sorting_prefix = 1;

SELECT 'sorting prefix, different constants';
SELECT g, n, x AS a, x AS b FROM t_interpolate_aliases_prefix ORDER BY g, n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS 1, b AS 2) SETTINGS enable_analyzer = 0, use_with_fill_by_sorting_prefix = 1; -- { serverError NOT_IMPLEMENTED }
SELECT g, n, x AS a, x AS b FROM t_interpolate_aliases_prefix ORDER BY g, n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (a AS 1, b AS 2) SETTINGS enable_analyzer = 1, use_with_fill_by_sorting_prefix = 1;

-- Aliases of the sorting prefix column are not interpolated, and cannot be an INTERPOLATE target.
SELECT g AS g1, g AS g2, n, x FROM t_interpolate_aliases_prefix ORDER BY g, n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE () SETTINGS enable_analyzer = 0, use_with_fill_by_sorting_prefix = 1;
SELECT g AS g1, g AS g2, n, x FROM t_interpolate_aliases_prefix ORDER BY g, n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE () SETTINGS enable_analyzer = 1, use_with_fill_by_sorting_prefix = 1;
SELECT g AS g1, g AS g2, n, x FROM t_interpolate_aliases_prefix ORDER BY g, n WITH FILL FROM 0 TO 2 STEP 0.5 INTERPOLATE (g1 AS 9) SETTINGS enable_analyzer = 0, use_with_fill_by_sorting_prefix = 1; -- { serverError INVALID_WITH_FILL_EXPRESSION }

DROP TABLE t_interpolate_aliases_prefix;
