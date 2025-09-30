DROP TABLE IF EXISTS tab;
CREATE TABLE tab ( `k` Nullable(UInt32), `k1` Nullable(UInt32), `k2` Nullable(UInt32), `v` String ) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES (1, 1, 1, 'a'), (2, 2, 2, 'b');

DROP TABLE IF EXISTS mem;
CREATE TABLE mem ( `k` UInt64, `v` String ) ENGINE = Join(ANY, LEFT, k);
INSERT INTO mem VALUES (1, 'A'), (2, 'B'), (3, 'B');

DROP TABLE IF EXISTS mem2;
CREATE TABLE mem2 ( `k` UInt64, `v` String ) ENGINE = Join(ANY, RIGHT, k);
INSERT INTO mem2 VALUES (1, 'A'), (2, 'B'), (3, 'B');

SET enable_analyzer = 1;

SELECT '-- no parallel replicas --';
SELECT *
FROM tab
ANY LEFT JOIN mem ON k1 = mem.k
ANY RIGHT JOIN mem2 ON k2 = mem2.k
ORDER BY tab.v
SETTINGS enable_parallel_replicas=0;

SELECT '-- parallel replicas --';
SELECT *
FROM tab
ANY LEFT JOIN mem ON k1 = mem.k
ANY RIGHT JOIN mem2 ON k2 = mem2.k
ORDER BY tab.v
SETTINGS enable_parallel_replicas=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree=1;


SELECT '-- explain: check parallel replicas is disabled, looking at ReadFromRemoteParallelReplicas steps count --';
SELECT count()
FROM
(
    EXPLAIN
    SELECT *
    FROM tab
    ANY LEFT JOIN mem ON k1 = mem.k
    ANY RIGHT JOIN mem2 ON k2 = mem2.k
    ORDER BY tab.v ASC
    SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1
)
WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';

DROP TABLE mem2;
DROP TABLE mem;
DROP TABLE tab;
