-- A comma join is admitted to parallel replicas like an `INNER` join: the query plan gives it keys from
-- `WHERE`, and a comma join left as a cross product still concatenates correctly from the split left side.

DROP TABLE IF EXISTS t1 SYNC;
DROP TABLE IF EXISTS t2 SYNC;
DROP TABLE IF EXISTS t3 SYNC;

CREATE TABLE t1 (c Int32) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t1', 'r1') ORDER BY c;
CREATE TABLE t2 (c Int32) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t2', 'r1') ORDER BY c;
CREATE TABLE t3 (c Int32) ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t3', 'r1') ORDER BY c;

INSERT INTO t1 SELECT number FROM numbers(10);
INSERT INTO t2 SELECT number * 2 FROM numbers(10);
INSERT INTO t3 VALUES (100), (200);

SET enable_analyzer = 1;
SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;
SET explain_query_plan_default = 'legacy';

SELECT '-- comma join with keys reads from remote replicas';
SELECT countIf(explain ILIKE '%ReadFromRemoteParallelReplicas%') > 0 FROM (
    EXPLAIN SELECT * FROM t1, t2, t3 WHERE t1.c = t2.c AND t2.c = t3.c ORDER BY ALL);

SELECT '-- results match the local execution, with and without a cross product';
SELECT * FROM t1, t2 WHERE t1.c = t2.c ORDER BY ALL;
SELECT * FROM t1, t2, t3 WHERE t1.c = t2.c ORDER BY ALL;
SELECT count() FROM t1, t2, t3 WHERE t1.c = t2.c;
SELECT count() FROM t1, t2, t3 WHERE t1.c = t2.c SETTINGS enable_parallel_replicas = 0;
SELECT count() FROM t1, t2, t3;
SELECT count() FROM t1, t2, t3 SETTINGS enable_parallel_replicas = 0;

DROP TABLE t1 SYNC;
DROP TABLE t2 SYNC;
DROP TABLE t3 SYNC;
