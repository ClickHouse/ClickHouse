-- Parallel `hash` splits many-matches, then JoinStep squash must not re-merge
-- past `max_joined_block_size_rows`. 03567 stays serial (right side below
-- `parallel_hash_join_threshold`). 03633 / 04102 zero `min_joined_block_size_*`,
-- which skips the squash. Leave min non-zero, force parallel, cap at 9.
-- Random settings limits: parallel_hash_join_threshold=(1, 1); max_threads=(4, 4); max_joined_block_size_rows=(9, 9); min_joined_block_size_rows=(65536, 65536); min_joined_block_size_bytes=(524288, 524288); joined_block_split_single_row=(1, 1); enable_lazy_columns_replication=(0, 0); enable_analyzer=(1, 1)

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET enable_lazy_columns_replication = 0;
SET explain_query_plan_default = 'legacy';
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

SELECT coalesce(
    nullIf(max(toUInt64OrZero(extract(explain, 'FillingRightJoinSide × (\\d+)'))), 0),
    countIf(explain LIKE '%FillingRightJoinSide%')) > 1
FROM (
    EXPLAIN PIPELINE
    SELECT l.k FROM t05052_l AS l INNER JOIN t05052_r AS r ON l.k = r.k
    SETTINGS max_threads = 4, query_plan_join_shard_by_pk_ranges = 0,
             join_algorithm = 'hash', parallel_hash_join_threshold = 1,
             query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
             enable_analyzer = 1, query_plan_join_swap_table = 0, enable_parallel_replicas = 0
);

SELECT if(max(blockSize()) > 9, 'Error: ' || toString(max(blockSize())), 'Ok'), count()
FROM t05052_l AS l INNER JOIN t05052_r AS r ON l.k = r.k
SETTINGS join_algorithm = 'hash', parallel_hash_join_threshold = 1, max_threads = 4,
         joined_block_split_single_row = 1, max_joined_block_size_rows = 9,
         min_joined_block_size_rows = 65536, min_joined_block_size_bytes = 524288,
         query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0,
         enable_parallel_replicas = 0, enable_join_runtime_filters = 0,
         enable_lazy_columns_replication = 0, enable_analyzer = 1,
         query_plan_join_shard_by_pk_ranges = 0;

DROP TABLE t05052_l;
DROP TABLE t05052_r;
