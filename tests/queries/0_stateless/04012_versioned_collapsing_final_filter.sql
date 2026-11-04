-- https://github.com/ClickHouse/ClickHouse/issues/80737
-- Pin the correctness-safe behavior this test guards. The partition key (c1) is not in the sorting
-- key (c2), so with `defer_partition_pruning_after_final = 0` the `c1 = 1` filter prunes away the
-- partition holding the cancelling sign = -1 row: FINAL then cannot collapse the pair and the last
-- query returns 2 instead of 1. That is the documented pre-26.3 behavior, unsafe for this shape.
SET defer_partition_pruning_after_final = 1;

CREATE TABLE t0 (c1 Int8, c2 Int) ENGINE = VersionedCollapsingMergeTree(c1, c2) ORDER BY (c2) PARTITION BY (c1);
INSERT INTO TABLE t0 (c1, c2) VALUES (-1,2),(1,2),(1,1);
SELECT c1, c2 FROM t0 FINAL;
SELECT '-';
SELECT count() FROM t0 FINAL;
SELECT '-';
SELECT count() FROM t0 FINAL WHERE c1 = 1;
