-- A distributed INSERT SELECT with parallel replicas ships the whole INSERT as a query to every
-- replica, and a replica executing that query never uses the plan-based implementation of parallel
-- replicas. The initiator takes part as one more replica, so `parallel_replicas_plan_based` must not
-- change how the insert is distributed: the number of executed INSERT queries stays the same as with
-- the query-based implementation.
--
-- `parallel_replicas_local_plan` decides whether the initiator reads and inserts its own share inside
-- its own pipeline, and the counts asserted below are the initiator's own `Insert` row plus one
-- shipped query per replica. Without a local pipeline the send loop skips nobody, so all three
-- replicas get a query - 3 + 1 = 4. With a local pipeline the local replica's pool is skipped -
-- 2 + 1 = 3. All three replicas of `test_cluster_one_shard_three_replicas_localhost` are this same
-- server, so every one of those rows lands in this `system.query_log`.

SET enable_analyzer = 1; -- parallel distributed insert select for replicated tables works only with analyzer
SET parallel_distributed_insert_select = 2;

DROP TABLE IF EXISTS t_mt_source;
DROP TABLE IF EXISTS t_rmt_target SYNC;

CREATE TABLE t_mt_source (k UInt64, v String) ENGINE = MergeTree() ORDER BY k;
CREATE TABLE t_rmt_target (k UInt64, v String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rmt_target', 'r1') ORDER BY ();

INSERT INTO t_mt_source SELECT number AS k, toString(number) AS v FROM system.numbers_mt LIMIT 1e5;
SELECT 'mt source table count()', count() FROM t_mt_source;

SET enable_parallel_replicas = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 1;

SELECT '-- without the local pipeline';
INSERT INTO t_rmt_target SELECT * FROM t_mt_source SETTINGS log_comment = '9cf0b9f3-8ab2-4f2a-9a1e-2ff0f1b27b1d', parallel_replicas_local_plan = 0;

SYSTEM FLUSH LOGS query_log;
SELECT count() FROM system.query_log WHERE (current_database = currentDatabase() OR has(databases, currentDatabase())) AND type = 'QueryFinish' AND query_kind = 'Insert' AND log_comment = '9cf0b9f3-8ab2-4f2a-9a1e-2ff0f1b27b1d' AND event_date >= yesterday() AND event_time >= now() - 600;

SELECT count() FROM t_rmt_target;
SELECT * FROM t_rmt_target ORDER BY k
EXCEPT
SELECT * FROM t_mt_source ORDER BY k;

SELECT '-- with the local pipeline';
TRUNCATE TABLE t_rmt_target;
INSERT INTO t_rmt_target SELECT * FROM t_mt_source SETTINGS log_comment = 'b1e4a5d0-3c6a-4f18-9a54-6a2d3f1c8e77', parallel_replicas_local_plan = 1;

SYSTEM FLUSH LOGS query_log;
SELECT count() FROM system.query_log WHERE (current_database = currentDatabase() OR has(databases, currentDatabase())) AND type = 'QueryFinish' AND query_kind = 'Insert' AND log_comment = 'b1e4a5d0-3c6a-4f18-9a54-6a2d3f1c8e77' AND event_date >= yesterday() AND event_time >= now() - 600;

SELECT count() FROM t_rmt_target;
SELECT * FROM t_rmt_target ORDER BY k
EXCEPT
SELECT * FROM t_mt_source ORDER BY k;

-- The implementation is pinned on a context the nested interpreter is handed, and that interpreter
-- re-applies the SELECT's own `SETTINGS` clause on top of it. Asking for the plan-based implementation
-- in the query text must not bring it back and stop the insert from being distributed.
SELECT '-- plan-based implementation asked for in the query text';
SET parallel_replicas_plan_based = 0;
TRUNCATE TABLE t_rmt_target;
INSERT INTO t_rmt_target SELECT * FROM t_mt_source SETTINGS log_comment = '4d2c7a16-5b9e-4c31-8f0d-7e6b5a4c3d2f', parallel_replicas_plan_based = 1, parallel_replicas_local_plan = 0;

SYSTEM FLUSH LOGS query_log;
SELECT count() FROM system.query_log WHERE (current_database = currentDatabase() OR has(databases, currentDatabase())) AND type = 'QueryFinish' AND query_kind = 'Insert' AND log_comment = '4d2c7a16-5b9e-4c31-8f0d-7e6b5a4c3d2f' AND event_date >= yesterday() AND event_time >= now() - 600;

SELECT count() FROM t_rmt_target;
SELECT * FROM t_rmt_target ORDER BY k
EXCEPT
SELECT * FROM t_mt_source ORDER BY k;

DROP TABLE t_mt_source;
DROP TABLE t_rmt_target SYNC;
