set enable_analyzer = 1;
set enable_parallel_replicas=1, max_parallel_replicas=2, parallel_replicas_local_plan=1;

select '-- SELECT';
SELECT * FROM (SELECT dummy AS k FROM remote('127.0.0.{1,2}', system.one));

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int, c1 Int) ENGINE MergeTree() ORDER BY tuple();
INSERT INTO t0 VALUES(0, 1);

select '-- UNION';
(
    SELECT 1 x, x y FROM remote('localhost', currentDatabase(), t0)
)
UNION ALL
(
    SELECT 1, c1 FROM t0 SETTINGS cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1
);

DROP TABLE t0;
