-- Tags: long
-- long: times out in private

SET log_queries = 1;
SET optimize_read_in_order=1;
SET use_skip_indexes_for_top_k = 0;
SET use_top_k_dynamic_filtering = 0;
DROP TABLE IF EXISTS read_in_order_with_parallel_replicas;
CREATE TABLE read_in_order_with_parallel_replicas(id UInt64) ENGINE=MergeTree ORDER BY id SETTINGS index_granularity=1;

SET max_execution_time = 300;
-- 1000 rows × index_granularity=1 = 1000 marks. Sufficient to verify the
-- read-in-order optimization (broken behavior would read all marks per replica
-- ≫ 2). Using fewer rows keeps INSERT fast under heavy sanitizer/WasmEdge load
-- where 100K marks could time out at INSERT.
INSERT INTO read_in_order_with_parallel_replicas SELECT number from system.numbers limit 1000;

SELECT * from read_in_order_with_parallel_replicas ORDER BY id desc limit 1;
SELECT * from read_in_order_with_parallel_replicas ORDER BY id limit 1;

SET automatic_parallel_replicas_mode = 0;
SET enable_analyzer=1, enable_parallel_replicas=2, max_parallel_replicas=2, cluster_for_parallel_replicas='parallel_replicas', parallel_replicas_for_non_replicated_merge_tree=1;

SELECT * from read_in_order_with_parallel_replicas ORDER BY id desc limit 1
SETTINGS max_threads=1, log_comment='test read in order desc with parallel replicas';

SELECT * from read_in_order_with_parallel_replicas ORDER BY id limit 1
SETTINGS max_threads=1, log_comment='test read in order asc with parallel replicas';

-- Check we don't read more mark in parallel replicas
SYSTEM FLUSH LOGS query_log;
SET parallel_replicas_for_non_replicated_merge_tree=0;
select count(1) from system.query_log where event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase() AND log_comment = 'test read in order desc with parallel replicas' and read_rows>2;
select count(1) from system.query_log where event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase() AND log_comment = 'test read in order asc with parallel replicas' and read_rows>2;

DROP TABLE read_in_order_with_parallel_replicas;
