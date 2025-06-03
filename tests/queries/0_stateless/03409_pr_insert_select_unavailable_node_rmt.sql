-- Tags: distributed

SET enable_analyzer=1; -- parallel distributed insert select for replicated tables works only with analyzer
SET parallel_distributed_insert_select=2;

-- use cluster with unavailable replica
SET enable_parallel_replicas = 1, cluster_for_parallel_replicas = 'parallel_replicas';

DROP TABLE IF EXISTS t_rmt_source SYNC;
DROP TABLE IF EXISTS t_rmt_target SYNC;

CREATE TABLE t_rmt_source (k UInt64, v String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rmt_source', 'r1') ORDER BY k;
CREATE TABLE t_rmt_target (k UInt64, v String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rmt_target', 'r1') ORDER BY ();

INSERT INTO t_rmt_source SELECT number as k, toString(number) as v FROM system.numbers LIMIT 1e6 settings parallel_distributed_insert_select=0;
select 'rmt source table count()', count() from t_rmt_source;

-- to ensure that unavailable replica will not be chosen in subsequent INSERT SELECTs, so query count from query_log will match the test expectations
SELECT k FROM t_rmt_source SETTINGS max_parallel_replicas=11 FORMAT Null;

select '-- check result without local pipeline';
INSERT INTO t_rmt_target SELECT * FROM t_rmt_source SETTINGS log_comment='d3be4828-e074-437b-adc1-605f05d96f6b', parallel_replicas_local_plan=0;

SYSTEM FLUSH LOGS query_log;
select count() from system.query_log where (current_database = currentDatabase() or has(databases, currentDatabase())) and type = 'QueryFinish' and query_kind = 'Insert' and log_comment='d3be4828-e074-437b-adc1-605f05d96f6b' and event_date >= yesterday();

select count() from t_rmt_target;
select * from t_rmt_target order by k
except
select * from t_rmt_source order by k;

select '-- check result with local pipeline';
TRUNCATE TABLE t_rmt_target;
INSERT INTO t_rmt_target SELECT * FROM t_rmt_source SETTINGS log_comment='4e33bcff-1d78-401b-83c0-ef454406a437', parallel_replicas_local_plan=1, parallel_replicas_insert_select_local_pipeline=1;

SYSTEM FLUSH LOGS query_log;
select count() from system.query_log where (current_database = currentDatabase() or has(databases, currentDatabase())) and type = 'QueryFinish' and query_kind = 'Insert' and log_comment='4e33bcff-1d78-401b-83c0-ef454406a437' and event_date >= yesterday();

select count() from t_rmt_target;
select * from t_rmt_target order by k
except
select * from t_rmt_source order by k;

DROP TABLE t_rmt_source SYNC;
DROP TABLE t_rmt_target SYNC;
