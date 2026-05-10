-- Tags: distributed
--
-- The `count_distinct_optimization` rewrite must be restricted to local tables.
-- It must NOT be applied to remote sources, regardless of whether they are
-- accessed via the `Distributed` table engine or the `remote(...)` table function:
-- the previous behavior (gating on `optimize_distributed_group_by_sharding_key`)
-- was replaced by a direct `IStorage::isRemote` check, which must cover both
-- `TableNode` and `TableFunctionNode` join trees.

DROP TABLE IF EXISTS data_04212;
DROP TABLE IF EXISTS dist_04212;

CREATE TABLE data_04212 (number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO data_04212 VALUES (1), (1), (2), (2), (3);
CREATE TABLE dist_04212 AS data_04212 ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), data_04212, number);

SET count_distinct_optimization = 1;
SET prefer_localhost_replica = 0;

-- Local table: the rewrite IS applied. The aggregate becomes `count`
-- and the join tree becomes a subquery with `GROUP BY`.
SELECT 'local table';
SELECT sum(countSubstrings(explain, 'function_name: count')) > 0 AS has_count_rewrite,
       sum(countSubstrings(explain, 'GROUP BY')) > 0 AS has_group_by_subquery,
       sum(countSubstrings(explain, 'function_name: uniqExact')) AS still_has_uniq_exact
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(DISTINCT number) FROM data_04212);

-- `Distributed` table: the rewrite is NOT applied. The aggregate stays as
-- `uniqExact` and the join tree stays as the original `TABLE` node.
SELECT 'distributed table';
SELECT sum(countSubstrings(explain, 'function_name: uniqExact')) > 0 AS keeps_uniq_exact,
       sum(countSubstrings(explain, 'GROUP BY')) AS no_extra_group_by
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(DISTINCT number) FROM dist_04212);

-- `remote(...)` table function: the rewrite is also NOT applied.
SELECT 'remote() function';
SELECT sum(countSubstrings(explain, 'function_name: uniqExact')) > 0 AS keeps_uniq_exact,
       sum(countSubstrings(explain, 'GROUP BY')) AS no_extra_group_by
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(DISTINCT number) FROM remote('127.0.0.{1,2}', currentDatabase(), data_04212));

-- Sanity check: results are still correct in all three cases.
SELECT 'results';
SELECT count(DISTINCT number) FROM data_04212;
SELECT count(DISTINCT number) FROM dist_04212;
SELECT count(DISTINCT number) FROM remote('127.0.0.{1,2}', currentDatabase(), data_04212);

DROP TABLE dist_04212;
DROP TABLE data_04212;
