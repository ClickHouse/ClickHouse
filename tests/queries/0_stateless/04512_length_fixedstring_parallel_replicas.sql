-- Regression for issue #110421: length(FixedString) collapsed to 0 on the parallel-replicas read path.

DROP TABLE IF EXISTS fsbug;
CREATE TABLE fsbug (id UInt32, fs FixedString(4)) ENGINE = MergeTree ORDER BY id;
INSERT INTO fsbug SELECT number, toFixedString(toString(1000 + number), 4) FROM numbers(100);

SET enable_parallel_replicas = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT length(fs) FROM fsbug WHERE id = 7;
SELECT length(fs) + 0 FROM fsbug WHERE id = 7;
SELECT materialize(length(fs)) FROM fsbug WHERE id = 7;
SELECT DISTINCT length(fs) FROM fsbug;

DROP TABLE fsbug;
