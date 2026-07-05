SET enable_analyzer = 1; --new analyzer is required for QUALIFY
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int, c1 Int) ENGINE = ReplacingMergeTree(c1) ORDER BY c0;
INSERT INTO t0 VALUES (1, 1);
SELECT c0 FROM t0 FINAL WHERE * OR materialize(1) QUALIFY isNotDistinctFrom(1, 256);
SELECT DISTINCT c0 FROM t0 FINAL WHERE * OR materialize(1);
SELECT DISTINCT c0 FROM t0 FINAL WHERE * OR materialize(1) QUALIFY isNotDistinctFrom(1, 256);
SELECT DISTINCT c0 FROM t0 FINAL QUALIFY isNotDistinctFrom(1, 256);
DROP TABLE t0;
