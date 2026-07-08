-- Tags: no-parallel
-- no-parallel: Creates the global SQL UDF `eval`, which is not isolated by the test database.

-- Regression test: under the old analyzer, the `WITH` literal substitution for the `eval` table function
-- must not rewrite a scalar SQL UDF call that happens to be named `eval`. Otherwise a query like
-- `WITH 10 AS q SELECT eval(q) FROM t` would have its argument rewritten before the query normalizer
-- could honour `prefer_column_name_to_alias`, so the source column `q` would be ignored.

DROP FUNCTION IF EXISTS eval;
CREATE FUNCTION eval AS x -> x + 1;

DROP TABLE IF EXISTS t_04492;
CREATE TABLE t_04492 (q UInt64) ENGINE = Memory;
INSERT INTO t_04492 VALUES (1);

-- Old analyzer, the source column wins: `eval` is applied to the column value `1`, so the result is `2`.
WITH 10 AS q SELECT eval(q) FROM t_04492 SETTINGS enable_analyzer = 0, prefer_column_name_to_alias = 1;

-- Old analyzer, the `WITH` alias wins (default): `eval` is applied to `10`, so the result is `11`.
WITH 10 AS q SELECT eval(q) FROM t_04492 SETTINGS enable_analyzer = 0, prefer_column_name_to_alias = 0;

-- New analyzer mirrors the old-analyzer behaviour above.
WITH 10 AS q SELECT eval(q) FROM t_04492 SETTINGS enable_analyzer = 1, prefer_column_name_to_alias = 1;
WITH 10 AS q SELECT eval(q) FROM t_04492 SETTINGS enable_analyzer = 1, prefer_column_name_to_alias = 0;

-- The `eval` table function must still substitute `WITH` literal aliases into its argument under the
-- old analyzer (this is the path that moved to the table-function position).
SET allow_experimental_eval_table_function = 1;
WITH 'SELECT 42 AS v' AS s SELECT * FROM eval(s) SETTINGS enable_analyzer = 0;
WITH 'SELECT 43 AS v' AS s SELECT * FROM eval(s) SETTINGS enable_analyzer = 1;

DROP TABLE t_04492;
DROP FUNCTION eval;
