-- https://github.com/ClickHouse/ClickHouse/issues/70779
-- Crash in SortCursorImpl with the old analyzer, which produces a block with 0 columns and 1 row
DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

SET allow_suspicious_primary_key = 1;

CREATE TABLE t0 (c0 Int) ENGINE = AggregatingMergeTree() ORDER BY tuple();
INSERT INTO TABLE t0 (c0) VALUES (1);
SELECT 42 FROM t0 FINAL PREWHERE t0.c0 = 1;
DROP TABLE t0;

CREATE TABLE t0 (c0 Int) ENGINE = SummingMergeTree() ORDER BY tuple();
INSERT INTO TABLE t0 (c0) VALUES (1);
SELECT 43 FROM t0 FINAL PREWHERE t0.c0 = 1;
DROP TABLE t0;

CREATE TABLE t0 (c0 Int) ENGINE = ReplacingMergeTree() ORDER BY tuple();
INSERT INTO TABLE t0 (c0) VALUES (1);
SELECT 44 FROM t0 FINAL PREWHERE t0.c0 = 1;
DROP TABLE t0;

CREATE TABLE t1 (a0 UInt8, c0 Int32, c1 UInt8) ENGINE = AggregatingMergeTree() ORDER BY tuple();
INSERT INTO TABLE t1 (a0, c0, c1) VALUES (1, 1, 1);
SELECT 45 FROM t1 FINAL PREWHERE t1.c0 = t1.c1;
DROP TABLE t1;
