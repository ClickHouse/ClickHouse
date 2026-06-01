-- Tags: shard
-- Distributed tables wrapped in `Merge` (engine or `merge()` function) must
-- still honour `optimize_skip_unused_shards`/`force_optimize_skip_unused_shards`
-- when the predicate is applied above the `Merge`.
-- Otherwise the predicate is pushed past `Distributed` and both shards
-- are queried, which (for self-referential clusters such as
-- `test_cluster_two_shards`) silently doubles the result.

SET enable_analyzer = 1;
SET optimize_skip_unused_shards = 1;

DROP TABLE IF EXISTS data1_04303;
DROP TABLE IF EXISTS data2_04303;
DROP TABLE IF EXISTS dist1_04303;
DROP TABLE IF EXISTS dist2_04303;
DROP TABLE IF EXISTS merge_04303;

CREATE TABLE data1_04303 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE data2_04303 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO data1_04303 SELECT number,        number * 10 FROM numbers(100);
INSERT INTO data2_04303 SELECT number + 1000, number * 10 FROM numbers(100);

CREATE TABLE dist1_04303 AS data1_04303 ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), data1_04303, k);
CREATE TABLE dist2_04303 AS data2_04303 ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), data2_04303, k);

CREATE TABLE merge_04303 AS data1_04303 ENGINE = Merge(currentDatabase(), '^dist[12]_04303$');

-- Baseline: a direct `Distributed` query prunes the unused shard.
SELECT 'direct', count() FROM dist1_04303 WHERE k = 5;
SELECT 'direct', count() FROM dist1_04303 WHERE k = 5 SETTINGS force_optimize_skip_unused_shards = 2;

-- `merge()` table function over two `Distributed` tables: predicate must reach
-- the sharding-key analysis of each `Distributed` child so that only one shard
-- is queried (count = 1, not 2).
SELECT 'merge_func', count() FROM merge(currentDatabase(), '^dist[12]_04303$') WHERE k = 5;
SELECT 'merge_func', count() FROM merge(currentDatabase(), '^dist[12]_04303$') WHERE k = 5 SETTINGS force_optimize_skip_unused_shards = 2;

-- Same via the `Merge` table engine.
SELECT 'merge_engine', count() FROM merge_04303 WHERE k = 5;
SELECT 'merge_engine', count() FROM merge_04303 WHERE k = 5 SETTINGS force_optimize_skip_unused_shards = 2;

DROP TABLE merge_04303;
DROP TABLE dist1_04303;
DROP TABLE dist2_04303;
DROP TABLE data1_04303;
DROP TABLE data2_04303;
