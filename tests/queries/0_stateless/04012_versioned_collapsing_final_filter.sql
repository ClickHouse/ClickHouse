-- https://github.com/ClickHouse/ClickHouse/issues/80737
CREATE TABLE t0 (c1 Int8, c2 Int) ENGINE = VersionedCollapsingMergeTree(c1, c2) ORDER BY (c2) PARTITION BY (c1);
INSERT INTO TABLE t0 (c1, c2) VALUES (-1,2),(1,2),(1,1);
SELECT c1, c2 FROM t0 FINAL;
SELECT '-';
SELECT count() FROM t0 FINAL;
SELECT '-';
SELECT count() FROM t0 FINAL WHERE c1 = 1;
