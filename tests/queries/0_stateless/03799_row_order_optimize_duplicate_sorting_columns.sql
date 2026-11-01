CREATE TABLE t0 (c0 Int, PROJECTION p0 (SELECT c0 GROUP BY c0, c0)) ENGINE = MergeTree() PRIMARY KEY tuple() SETTINGS optimize_row_order = 1, allow_suspicious_indices = 1;
INSERT INTO TABLE t0 (c0) SELECT number FROM numbers(10);
OPTIMIZE TABLE t0 FINAL;
SELECT * FROM t0 ORDER BY ALL;
