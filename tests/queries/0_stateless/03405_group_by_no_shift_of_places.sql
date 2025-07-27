CREATE TABLE t(a Int64) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO t VALUES (3), (4), (5), (6), (1), (2);
SELECT *, sum(a) FROM t GROUP BY a ORDER BY a LIMIT 2;
