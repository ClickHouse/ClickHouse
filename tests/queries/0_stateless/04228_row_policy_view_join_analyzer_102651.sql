-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/102651
-- JOIN of a table with a row policy and a view used to fail under the analyzer with
-- `NOT_FOUND_COLUMN_IN_BLOCK` for a column referenced by the row policy expression.

DROP ROW POLICY IF EXISTS r_102651 ON t1;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP VIEW IF EXISTS v;

CREATE TABLE t1 (col Int64, col2 Nullable(Int64), col3 Int64) ENGINE = MergeTree() ORDER BY col;
CREATE ROW POLICY r_102651 ON t1 USING col2 IS NOT NULL TO ALL;
INSERT INTO t1 (col, col2, col3) VALUES (0, NULL, 0);

CREATE TABLE t2 (col Int64) ENGINE = MergeTree() ORDER BY col;
CREATE VIEW v (col Int64) AS SELECT col FROM t2;

SELECT col FROM t1 JOIN t2 ON (t1.col = t2.col) ORDER BY col SETTINGS enable_analyzer = 1;
SELECT col FROM t1 JOIN v ON (t1.col = v.col) ORDER BY col SETTINGS enable_analyzer = 0;
SELECT col FROM t1 JOIN v ON (t1.col = v.col) ORDER BY col SETTINGS enable_analyzer = 1;

DROP ROW POLICY r_102651 ON t1;
DROP VIEW v;
DROP TABLE t2;
DROP TABLE t1;
