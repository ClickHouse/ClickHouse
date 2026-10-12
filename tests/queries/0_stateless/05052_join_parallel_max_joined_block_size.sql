-- A hash join built in parallel splits a key with many matches into blocks of at most
-- `max_joined_block_size_rows` rows, and the squash after the join must not merge them
-- past that size. A non-zero `min_joined_block_size_*` keeps that squash on.
-- Random settings limits: parallel_hash_join_threshold=(1, 1); max_threads=(4, 4); max_joined_block_size_rows=(9, 9); min_joined_block_size_rows=(65536, 65536); min_joined_block_size_bytes=(524288, 524288); joined_block_split_single_row=(1, 1); enable_lazy_columns_replication=(0, 0)

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 0;
SET query_plan_join_shard_by_pk_ranges = 0;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET enable_lazy_columns_replication = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET join_algorithm = 'hash';
SET parallel_hash_join_threshold = 1;
SET max_threads = 4;
SET joined_block_split_single_row = 1;
SET max_joined_block_size_rows = 9;
SET min_joined_block_size_rows = 65536;
SET min_joined_block_size_bytes = 524288;

DROP TABLE IF EXISTS t05052_l;
DROP TABLE IF EXISTS t05052_r;
CREATE TABLE t05052_l (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t05052_r (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t05052_l SELECT number FROM numbers(20);
INSERT INTO t05052_r SELECT number % 20 FROM numbers(2000);

SELECT if(max(blockSize()) > 9, 'Error: ' || toString(max(blockSize())), 'Ok'), count()
FROM t05052_l AS l INNER JOIN t05052_r AS r ON l.k = r.k
SETTINGS log_comment = '05052_split';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['HashJoinBuiltWithSerialLayout'] AS serial, ProfileEvents['HashJoinBuiltWithParallelLayout'] AS parallel
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05052_split';

DROP TABLE t05052_l;
DROP TABLE t05052_r;
