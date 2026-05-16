SET enable_analyzer = 1;

CREATE TABLE t (x LowCardinality(String)) ENGINE=Memory;
INSERT INTO t VALUES ('a');
SELECT x IS NULL, x FROM t GROUP BY x WITH ROLLUP SETTINGS group_by_use_nulls=1;

-- See https://github.com/ClickHouse/ClickHouse/issues/95299
-- Reproduce CI failures: GROUPING SETS and CUBE with LowCardinality keys.
SELECT '---';
SELECT x IS NULL, x FROM t GROUP BY GROUPING SETS ((x), ()) ORDER BY ALL SETTINGS group_by_use_nulls=1;
SELECT '---';
SELECT x IS NULL, x FROM t GROUP BY x WITH CUBE ORDER BY ALL SETTINGS group_by_use_nulls=1;

CREATE TABLE t_uint (k LowCardinality(UInt16)) ENGINE=Memory;
INSERT INTO t_uint VALUES (1024);
SELECT '---';
SELECT k IS NULL, k FROM t_uint GROUP BY k WITH ROLLUP ORDER BY ALL SETTINGS group_by_use_nulls=1;
SELECT '---';
SELECT k IS NULL, k FROM t_uint GROUP BY GROUPING SETS ((k), ()) ORDER BY ALL SETTINGS group_by_use_nulls=1;
SELECT '---';
SELECT k IS NULL, k FROM t_uint GROUP BY k WITH CUBE ORDER BY ALL SETTINGS group_by_use_nulls=1;
