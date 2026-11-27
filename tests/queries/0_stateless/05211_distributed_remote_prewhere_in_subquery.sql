-- Tags: distributed
-- Wrapping a remote non-leftmost table must move `PREWHERE` (including `GLOBAL IN`)
-- onto that subquery exclusively, so the outer planner does not keep the
-- original not-ready `Set`.
-- https://github.com/ClickHouse/ClickHouse/pull/119780

SET enable_analyzer = 1;
SET join_use_nulls = 1;

DROP TABLE IF EXISTS left_local_05211;
DROP TABLE IF EXISTS right_local_05211;
DROP TABLE IF EXISTS right_distributed_05211;

CREATE TABLE left_local_05211 (k1 UInt32, v1 String)
ENGINE = MergeTree
ORDER BY k1;

CREATE TABLE right_local_05211 (k2 UInt32, v2 String)
ENGINE = MergeTree
ORDER BY k2;

CREATE TABLE right_distributed_05211 AS right_local_05211
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), right_local_05211);

INSERT INTO left_local_05211 VALUES (1, 'a'), (2, 'b');
INSERT INTO right_local_05211 VALUES (1, 'A'), (2, 'B'), (3, 'C');

SELECT l.k1, l.v1, r.k2, r.v2, count()
FROM left_local_05211 AS l
INNER JOIN right_distributed_05211 AS r ON l.k1 = r.k2
PREWHERE r.k2 GLOBAL IN (SELECT k2 FROM right_distributed_05211 WHERE k2 = 1)
GROUP BY ALL
ORDER BY ALL;

DROP TABLE right_distributed_05211;
DROP TABLE left_local_05211;
DROP TABLE right_local_05211;
