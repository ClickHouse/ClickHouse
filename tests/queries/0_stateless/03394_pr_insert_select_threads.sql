-- Tags: long, no-parallel, no-object-storage, no-msan, no-tsan

SET enable_analyzer=1; -- parallel distributed insert select for replicated tables works only with analyzer
SET parallel_distributed_insert_select=2;

DROP TABLE IF EXISTS t_mt_source;
DROP TABLE IF EXISTS t_rmt_target SYNC;

CREATE TABLE t_mt_source (k UInt64, v String) ENGINE = MergeTree() ORDER BY k SETTINGS index_granularity=10;
CREATE TABLE t_rmt_target (k UInt64, v String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rmt_target', 'r1') ORDER BY ();

SYSTEM STOP MERGES t_mt_source;
INSERT INTO t_mt_source SELECT number as k, toString(number) as v FROM system.numbers_mt LIMIT 1e6 SETTINGS max_block_size=1000, min_insert_block_size_rows=1000;
SELECT count() FROM system.parts WHERE database = currentDatabase() and table = 't_mt_source';

SET cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost', max_parallel_replicas=3, parallel_replicas_for_non_replicated_merge_tree=1, parallel_replicas_mark_segment_size=128;

-- we do not want concurrency control to limit the number of threads
SET use_concurrency_control=0;

-- reduce block size to ensure that all threads will be used
INSERT INTO t_rmt_target SELECT * FROM t_mt_source SETTINGS allow_experimental_parallel_reading_from_replicas=1, max_threads=8, max_insert_threads=4, max_block_size=1000, min_insert_block_size_rows=1000, parallel_replicas_local_plan=0;
INSERT INTO t_rmt_target SELECT * FROM t_mt_source SETTINGS allow_experimental_parallel_reading_from_replicas=1, max_threads=8, max_insert_threads=4, max_block_size=1000, min_insert_block_size_rows=1000, parallel_replicas_local_plan=1, parallel_replicas_insert_select_local_pipeline=1;

SYSTEM FLUSH LOGS query_log;
SELECT
    if(is_initial_query, 'inital', 'secondary'),
    if(toUInt64OrZero(Settings['parallel_replicas_insert_select_local_pipeline']) == 0 and is_initial_query, 1, 8) threads_limit,
    least(peak_threads_usage, threads_limit),
    format('local_pipeline={}', Settings['parallel_replicas_insert_select_local_pipeline'])
FROM system.query_log
WHERE (current_database = currentDatabase() OR has(databases, currentDatabase())) AND type = 'QueryFinish' AND Settings['allow_experimental_parallel_reading_from_replicas']='1' AND query_kind = 'Insert' AND has(tables, currentDatabase() || '.t_rmt_target')
ORDER BY event_time_microseconds
SETTINGS allow_experimental_parallel_reading_from_replicas=0
;

DROP TABLE t_mt_source;
DROP TABLE t_rmt_target SYNC;
