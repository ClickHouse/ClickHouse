SET enable_analyzer = 1;

CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE t1 (c0 Int) ENGINE = Distributed('test_cluster_two_shards', default, t0);
SELECT (SELECT _shard_num) FROM t1 GROUP BY _shard_num SETTINGS allow_experimental_correlated_subqueries = 1; -- { serverError NOT_IMPLEMENTED }
