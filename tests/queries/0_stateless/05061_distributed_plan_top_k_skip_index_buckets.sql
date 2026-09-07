-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.
-- Tracked in https://github.com/ClickHouse/ClickHouse/issues/118534

-- `ORDER BY <non-key column> LIMIT n` over a table with a minmax skip index on the sort column, read
-- as a bucketed distributed read. The coordinator plans without the top-K optimization (`tryOptimizeTopK`
-- is skipped while `make_distributed_plan = 1`) and pins each bucket's marks over the full part set.
-- The worker re-optimizes its fragment with `make_distributed_plan = 0`, so `tryOptimizeTopK` applies
-- there and `filterPartsByPrimaryKeyAndSkipIndexes` narrows the read to the top-K granules of the minmax
-- index, erasing every part that keeps no granule, before `initializePipeline` resolves the pinned bucket
-- marks by part name. A bucket that references an erased part must not fail with `NO_SUCH_DATA_PART`.
--
-- `index_granularity = 64` is pinned on the table: with one granule per part the top-K pruning keeps all
-- three granules (k = 5 > 3) and no part is erased. `distributed_plan_max_rows_to_broadcast = 0` buckets
-- the small table. `use_skip_indexes_for_top_k` and `query_plan_max_limit_for_top_k_optimization` are
-- randomized by the test runner, so they are pinned to keep the top-K skip-index path active.

SET enable_analyzer = 1, enable_parallel_replicas = 0;
SET use_skip_indexes = 1, use_skip_indexes_for_top_k = 1, query_plan_max_limit_for_top_k_optimization = 0;

DROP TABLE IF EXISTS t_top_k_buckets;
CREATE TABLE t_top_k_buckets (k UInt32, v UInt64, s String, INDEX idx_v v TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
INSERT INTO t_top_k_buckets SELECT number, number, toString(number) FROM numbers(3000);
INSERT INTO t_top_k_buckets SELECT number, number, toString(number) FROM numbers(3000, 3000);
INSERT INTO t_top_k_buckets SELECT number, number, toString(number) FROM numbers(6000, 3000);

SELECT '-- local';
SELECT k, v FROM t_top_k_buckets ORDER BY v LIMIT 5;

SELECT '-- distributed, ORDER BY v ASC';
SELECT k, v FROM t_top_k_buckets ORDER BY v LIMIT 5
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;

SELECT '-- distributed, ORDER BY v DESC';
SELECT k, v FROM t_top_k_buckets ORDER BY v DESC LIMIT 5
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;

SELECT '-- distributed, ORDER BY v, k LIMIT 5 OFFSET 2';
SELECT k, v FROM t_top_k_buckets ORDER BY v, k LIMIT 5 OFFSET 2
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;

SELECT '-- distributed, sort column only';
SELECT v FROM t_top_k_buckets ORDER BY v LIMIT 5
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;

-- Strict mode: the same query must really run as a distributed (bucketed) read, not silently fall back.
SELECT '-- distributed, strict (no fallback to local execution)';
SELECT k, v FROM t_top_k_buckets ORDER BY v LIMIT 5
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0,
    distributed_plan_fallback_to_local_execution = 0;

-- Control: without the top-K skip-index path the worker's analysis matches the coordinator's.
SELECT '-- distributed, use_skip_indexes_for_top_k = 0';
SELECT k, v FROM t_top_k_buckets ORDER BY v LIMIT 5
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0,
    distributed_plan_fallback_to_local_execution = 0, use_skip_indexes_for_top_k = 0;

DROP TABLE t_top_k_buckets;
