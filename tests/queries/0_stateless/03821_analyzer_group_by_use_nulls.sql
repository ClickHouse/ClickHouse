CREATE TABLE t (x LowCardinality(String)) ENGINE=Memory;
INSERT INTO t VALUES ('a');
SELECT x IS NULL, x FROM t GROUP BY x WITH ROLLUP SETTINGS group_by_use_nulls=1;
