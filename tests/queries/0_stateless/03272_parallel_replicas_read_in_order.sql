SET log_queries = 1;
SET optimize_read_in_order=1;
DROP TABLE IF EXISTS read_in_order_with_parallel_replicas;
CREATE TABLE read_in_order_with_parallel_replicas(id UInt64) ENGINE=MergeTree ORDER BY id SETTINGS index_granularity=1;


INSERT INTO read_in_order_with_parallel_replicas SELECT number from system.numbers limit 100000;

SELECT * from read_in_order_with_parallel_replicas ORDER BY id desc limit 1 settings max_threads=1;
SELECT * from read_in_order_with_parallel_replicas ORDER BY id limit 1 settings max_threads=1;


SELECT * from read_in_order_with_parallel_replicas ORDER BY id desc limit 1
SETTINGS max_parallel_replicas = 2,
enable_parallel_replicas = 1,
cluster_for_parallel_replicas='parallel_replicas',
parallel_replicas_for_non_replicated_merge_tree=1,
max_threads=1,
log_comment='test read in order desc with parallel replicas';

SELECT * from read_in_order_with_parallel_replicas ORDER BY id limit 1
SETTINGS max_parallel_replicas = 2,
enable_parallel_replicas = 1,
cluster_for_parallel_replicas='parallel_replicas',
parallel_replicas_for_non_replicated_merge_tree=1,
max_threads=1,
log_comment='test read in order asc with parallel replicas';

-- Check we don't read more mark in parallel replicas
SYSTEM FLUSH LOGS;
select count(1) from system.query_log where current_database = currentDatabase() AND log_comment = 'test read in order desc with parallel replicas' and read_rows>2;
select count(1) from system.query_log where current_database = currentDatabase() AND log_comment = 'test read in order asc with parallel replicas' and read_rows>2;

DROP TABLE IF EXISTS read_in_order_with_parallel_replicas;
