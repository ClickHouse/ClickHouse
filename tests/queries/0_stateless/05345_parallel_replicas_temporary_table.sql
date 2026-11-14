-- Only `Memory` temporary tables are sent to the replicas, so a query that would make a replica read another temporary
-- table runs without parallel replicas.

DROP TABLE IF EXISTS t_regular;
CREATE TABLE t_regular (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_regular SELECT number FROM numbers(20);

CREATE TEMPORARY TABLE t_temporary (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_temporary SELECT number FROM numbers(10);
CREATE TEMPORARY TABLE t_memory (a UInt64) ENGINE = Memory;
INSERT INTO t_memory SELECT number FROM numbers(10);

CREATE TEMPORARY VIEW v_temporary AS SELECT a FROM t_regular;

SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, automatic_parallel_replicas_mode = 0;

SELECT sum(a) FROM t_temporary SETTINGS parallel_replicas_local_plan = 1;
SELECT sum(a) FROM t_temporary SETTINGS parallel_replicas_local_plan = 0;
SELECT count() FROM t_regular AS r INNER JOIN t_temporary AS t ON r.a = t.a SETTINGS parallel_replicas_local_plan = 0;
SELECT count() FROM t_regular WHERE a IN (SELECT a FROM t_temporary) SETTINGS parallel_replicas_local_plan = 0;
SELECT sum(a) FROM t_temporary SETTINGS parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'a';
SELECT count() FROM t_regular WHERE a GLOBAL IN (SELECT a FROM t_temporary) SETTINGS parallel_replicas_local_plan = 0;
SELECT sum(a) FROM v_temporary SETTINGS parallel_replicas_allow_view_over_mergetree = 1, parallel_replicas_local_plan = 1;
SELECT * FROM (SELECT sum(a) FROM t_temporary SETTINGS enable_parallel_replicas = 1, parallel_replicas_local_plan = 0) SETTINGS enable_parallel_replicas = 0;
SELECT sum(a) FROM view(SELECT a FROM t_temporary) SETTINGS parallel_replicas_allow_view_over_mergetree = 1, parallel_replicas_local_plan = 0;
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t_regular WHERE a IN t_memory SETTINGS parallel_replicas_local_plan = 1) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';

DROP TEMPORARY VIEW v_temporary;
DROP TABLE t_regular;
