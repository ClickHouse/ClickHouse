DROP TABLE IF EXISTS t_dist_const_sort_key;

CREATE TABLE t_dist_const_sort_key (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_dist_const_sort_key;
INSERT INTO t_dist_const_sort_key SELECT number * 3, number FROM numbers(30000);
INSERT INTO t_dist_const_sort_key SELECT number * 3 + 1, number FROM numbers(30000);
INSERT INTO t_dist_const_sort_key SELECT number * 3 + 2, number FROM numbers(30000);

SET distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;
SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0;
SET automatic_parallel_replicas_mode = 0;
SET optimize_read_in_order = 1, read_in_order_use_virtual_row = 0, distributed_plan_read_in_order = 1;

-- The constant sort key is computed below the exchange that feeds the sorting stage. `sleepEachRow` paces the read so that the
-- sorting stage, after keeping its LIMIT threshold from a chunk, waits for the next one; `max_block_size` makes every part several blocks.
SELECT k FROM t_dist_const_sort_key ARRAY JOIN [1, 2] AS x
WHERE sleepEachRow(0.000005) = 0
ORDER BY toUInt8(1), k
LIMIT 10 OFFSET 1990
SETTINGS max_block_size = 8192, distributed_plan_fallback_to_local_execution = 0;

DROP TABLE t_dist_const_sort_key;
