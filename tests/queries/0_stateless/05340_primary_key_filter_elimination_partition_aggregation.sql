-- Exercise local filter elimination alongside independent aggregation in the distributed-plan suite.
SET serialize_query_plan = 0,
    enable_parallel_replicas = 0,
    optimize_use_projections = 0,
    optimize_use_implicit_projections = 0,
    optimize_read_in_order = 0,
    optimize_aggregation_in_order = 0,
    optimize_move_to_prewhere = 0,
    compile_expressions = 0,
    use_query_condition_cache = 0,
    allow_aggregate_partitions_independently = 1,
    force_aggregate_partitions_independently = 1,
    max_rows_to_group_by = 0,
    max_threads = 2,
    max_block_size = 7,
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    query_plan_optimize_primary_key_filter_elimination = 1;

DROP TABLE IF EXISTS pk_filter_partitioned;
CREATE TABLE pk_filter_partitioned (pid UInt8, k UInt64, amount UInt64)
ENGINE = MergeTree PARTITION BY pid ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES pk_filter_partitioned;
INSERT INTO pk_filter_partitioned
SELECT number % 4, intDiv(number, 4), number + 1 FROM numbers(1024)
SETTINGS max_insert_threads = 1, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

-- `PREWHERE` and a nontrivial aggregate argument retain the expression shape
-- required by partition-wise aggregation. A `Union` introduced earlier would
-- hide the reader and silently defeat the explicitly requested optimization.
SELECT 'disabled_plan',
    countIf(explain LIKE '%Skip merging: 1%') > 0,
    countIf(explain LIKE '%Read each partition through separate port: 1%') > 0,
    countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT pid, sum(amount + 1), count() FROM pk_filter_partitioned
    PREWHERE k >= 13 AND k < 243 GROUP BY pid ORDER BY pid
    SETTINGS query_plan_optimize_primary_key_filter_elimination = 0);
SELECT 'enabled_plan',
    countIf(explain LIKE '%Skip merging: 1%') > 0,
    countIf(explain LIKE '%Read each partition through separate port: 1%') > 0,
    countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT pid, sum(amount + 1), count() FROM pk_filter_partitioned
    PREWHERE k >= 13 AND k < 243 GROUP BY pid ORDER BY pid);

SELECT 'disabled', pid, sum(amount + 1), count() FROM pk_filter_partitioned
PREWHERE k >= 13 AND k < 243 GROUP BY pid ORDER BY pid
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0;
SELECT 'enabled', pid, sum(amount + 1), count() FROM pk_filter_partitioned
PREWHERE k >= 13 AND k < 243 GROUP BY pid ORDER BY pid;

-- Without partition keys in `GROUP BY`, the independent aggregation cannot
-- apply. The filter optimization can still benefit this partitioned table.
SELECT 'ungrouped_plan', countIf(explain LIKE '%Primary key proven filter%') > 0
FROM (EXPLAIN description = 1 SELECT sum(amount + 1) FROM pk_filter_partitioned
    PREWHERE k >= 13 AND k < 243);
SELECT 'ungrouped', sum(amount + 1) FROM pk_filter_partitioned
PREWHERE k >= 13 AND k < 243;

DROP TABLE pk_filter_partitioned;
