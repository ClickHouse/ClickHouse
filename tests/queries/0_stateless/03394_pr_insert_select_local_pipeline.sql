SET enable_analyzer=1; -- parallel distributed insert select for replicated tables works only with analyzer
SET parallel_distributed_insert_select=2;

DROP TABLE IF EXISTS t_mt_source;
DROP TABLE IF EXISTS t_mt_target;

CREATE TABLE t_mt_source (k UInt64, v String) ENGINE = MergeTree() ORDER BY k;
CREATE TABLE t_mt_target (k UInt64, v String) ENGINE = MergeTree() ORDER BY ();

INSERT INTO t_mt_source SELECT number as k, toString(number) as v FROM system.numbers LIMIT 1e6;
select 'mt source table count()', count() from t_mt_source;

SET enable_parallel_replicas = 1, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas = 3;

select '-- check result with local pipeline';
TRUNCATE TABLE t_mt_target;
INSERT INTO t_mt_target SELECT * FROM t_mt_source SETTINGS log_comment='c1fcb43d-1703-4ddb-b353-c8079b405c16', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan=1, parallel_replicas_insert_select_local_pipeline=1;

SYSTEM FLUSH LOGS query_log;
select count() from system.query_log where (current_database = currentDatabase() or has(databases, currentDatabase())) and type = 'QueryFinish' and query_kind = 'Insert' and log_comment='c1fcb43d-1703-4ddb-b353-c8079b405c16' and event_date >= yesterday();

select count() from t_mt_target;
select * from t_mt_target order by k
except
select * from t_mt_source order by k;

DROP TABLE t_mt_source;
DROP TABLE t_mt_target;
