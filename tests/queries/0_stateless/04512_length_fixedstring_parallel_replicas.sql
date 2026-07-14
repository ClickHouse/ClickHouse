-- Tags: no-parallel-replicas
-- length(FixedString) is a constant derived from the type (the fixed size N). It must survive the
-- parallel-replicas read path even with a selective filter that returns a single row. Regression for
-- issue #110421 where the folded constant collapsed to 0 during 0-row header evaluation.

DROP TABLE IF EXISTS fsbug;
CREATE TABLE fsbug (id UInt32, fs FixedString(4)) ENGINE = MergeTree ORDER BY id;
INSERT INTO fsbug SELECT number, toFixedString(toString(1000 + number), 4) FROM numbers(100);

SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_only_with_analyzer = 0;

SELECT length(fs) FROM fsbug WHERE id = 7;
SELECT length(fs) + 0 FROM fsbug WHERE id = 7;
SELECT materialize(length(fs)) FROM fsbug WHERE id = 7;
SELECT DISTINCT length(fs) FROM fsbug;

DROP TABLE fsbug;
