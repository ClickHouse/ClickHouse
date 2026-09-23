-- https://github.com/ClickHouse/ClickHouse/issues/91819
-- A `Map` with a `LowCardinality` key on `CoalescingMergeTree`, which aggregates every non-key
-- column and rebuilds its declared type after the merge. `optimize_on_insert` gates the
-- insert-time merge, so it is pinned against the runner's randomization; `FINAL` over the two
-- inserts covers the read-time merge.
SET allow_suspicious_low_cardinality_types = 1;
SET optimize_on_insert = 1;

CREATE TABLE t0 (c0 Int, c1 Map(LowCardinality(Int), Int)) ENGINE = CoalescingMergeTree ORDER BY c0;
INSERT INTO TABLE t0 (c0, c1) VALUES (1, map(2, 3));
INSERT INTO TABLE t0 (c0, c1) VALUES (1, map(4, 5));
SELECT c0, c1, toTypeName(c1) FROM t0 FINAL ORDER BY ALL;
DROP TABLE t0;
