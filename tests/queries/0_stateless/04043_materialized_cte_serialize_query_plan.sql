SET enable_analyzer = 1;
SET enable_materialized_cte = 1;
SET serialize_query_plan = 1;

-- Materialized CTE with serialize_query_plan must not fail with
-- "Method serialize is not implemented for DelayedMaterializingCTEs".
-- The DelayedMaterializingCTEsStep is skipped during plan serialization
-- because CTEs are materialized on the initiator.

SELECT '-- cluster() table function';

WITH t AS MATERIALIZED (SELECT number AS x FROM numbers(7))
SELECT *
FROM cluster(test_cluster_two_shards, numbers(10))
WHERE number IN (SELECT * FROM t WHERE x > 5) OR number IN (SELECT * FROM t WHERE x > 5)
ORDER BY number;

SELECT '-- StorageDistributed';

DROP TABLE IF EXISTS local_04043;
DROP TABLE IF EXISTS dist_04043;

CREATE TABLE local_04043 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dist_04043 AS local_04043 ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), local_04043);

INSERT INTO local_04043 SELECT number FROM numbers(100);

WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(50))
SELECT count() FROM dist_04043
WHERE x IN (t) OR x IN (t);

WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(50))
SELECT count() FROM dist_04043
WHERE x IN (SELECT c FROM t WHERE c < 10) OR x IN (SELECT c FROM t WHERE c >= 10 AND c < 50);

DROP TABLE dist_04043;
DROP TABLE local_04043;

SELECT '-- parallel replicas';

DROP TABLE IF EXISTS pr_04043;
CREATE TABLE pr_04043 (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO pr_04043 SELECT number FROM numbers(100);

WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(50))
SELECT count() FROM pr_04043
WHERE x IN (t) OR x IN (t)
SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 1;

WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(50))
SELECT count() FROM pr_04043
WHERE x IN (SELECT c FROM t WHERE c < 10) OR x IN (SELECT c FROM t WHERE c >= 10 AND c < 50)
SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 1;

DROP TABLE pr_04043;
