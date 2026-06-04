-- Tags: no-old-analyzer

-- A nonzero max_rows_to_group_by keeps aggregation single-node, so pin it to 0.
SET max_rows_to_group_by = 0;
SET distributed_plan_optimize_exchanges = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET query_plan_remove_unused_columns = 1;
CREATE TABLE test(src_ip UInt32, dst_ip UInt32, bytes UInt64) ENGINE MergeTree() ORDER BY src_ip settings auto_statistics_types='';

INSERT INTO test SELECT number%30, (number+10)%30, number%50 FROM numbers(100);
INSERT INTO test SELECT number%30, (number+10)%30, number%50 FROM numbers(100, 100);

-- t1.src_ip!=0 condition is not moved to prewhere because src_ip is in primary key

SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 1;

SELECT '-------------------------';

EXPLAIN
SELECT count() FROM test AS t1 JOIN test AS t2 ON t1.src_ip = t2.dst_ip WHERE t1.src_ip != 0 AND t1.bytes > 10
SETTINGS make_distributed_plan=1, enable_parallel_replicas=0, distributed_plan_optimize_exchanges=0, distributed_plan_max_rows_to_broadcast=0;

SELECT '-------------------------';

EXPLAIN
SELECT count() FROM test AS t1 JOIN test AS t2 ON t1.src_ip = t2.dst_ip WHERE t1.src_ip != 0 AND t1.bytes > 10
SETTINGS make_distributed_plan=1, enable_parallel_replicas=0, distributed_plan_max_rows_to_broadcast=0;

SELECT '-------------------------';

SELECT count() FROM test AS t1 JOIN test AS t2 ON t1.src_ip = t2.dst_ip WHERE t1.src_ip != 0 AND t1.bytes > 10
SETTINGS make_distributed_plan=1, enable_parallel_replicas=0, distributed_plan_optimize_exchanges=0, distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;

SELECT count() FROM test AS t1 JOIN test AS t2 ON t1.src_ip = t2.dst_ip WHERE t1.src_ip != 0 AND t1.bytes > 10
SETTINGS make_distributed_plan=1, enable_parallel_replicas=0, distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;

SELECT count() FROM test AS t1 JOIN test AS t2 ON t1.src_ip = t2.dst_ip WHERE t1.src_ip != 0 AND t1.bytes > 10
SETTINGS make_distributed_plan=0;
