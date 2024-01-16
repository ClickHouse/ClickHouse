CREATE TABLE t (k1 UInt64, k2 UInt64, v UInt64) ENGINE = ReplacingMergeTree() ORDER BY (k1, k2);
SET optimize_on_insert = 0;
INSERT INTO t VALUES (1, 2, 3) (1, 2, 4) (2, 3, 4), (2, 3, 5);
-- { echo ON }
SELECT arrayJoin([(k1, v), (k2, v)]) AS row, row.1 as k FROM t FINAL WHERE k1 != 3 AND k = 1 ORDER BY row SETTINGS enable_vertical_final = 0;
SELECT arrayJoin([(k1, v), (k2, v)]) AS row, row.1 as k FROM t FINAL WHERE k1 != 3 AND k = 1 ORDER BY row SETTINGS enable_vertical_final = 1;
SELECT arrayJoin([(k1, v), (k2, v)]) AS row, row.1 as k FROM t FINAL WHERE k1 != 3 AND k = 2 ORDER BY row SETTINGS enable_vertical_final = 0;
SELECT arrayJoin([(k1, v), (k2, v)]) AS row, row.1 as k FROM t FINAL WHERE k1 != 3 AND k = 2 ORDER BY row SETTINGS enable_vertical_final = 1;
SELECT arrayJoin([(k1, v), (k2, v)]) AS row, row.1 as k FROM t FINAL WHERE k1 != 3 AND k = 3 ORDER BY row SETTINGS enable_vertical_final = 0;
SELECT arrayJoin([(k1, v), (k2, v)]) AS row, row.1 as k FROM t FINAL WHERE k1 != 3 AND k = 3 ORDER BY row SETTINGS enable_vertical_final = 1;
