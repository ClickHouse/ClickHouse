SET serialize_query_plan = 0,
    enable_parallel_replicas = 0,
    optimize_use_projections = 0,
    optimize_use_implicit_projections = 0,
    optimize_read_in_order = 0,
    optimize_aggregation_in_order = 0,
    optimize_move_to_prewhere = 0,
    compile_expressions = 0,
    use_query_condition_cache = 0,
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    max_threads = 1,
    max_block_size = 128,
    query_plan_optimize_primary_key_filter_elimination = 1;

DROP TABLE IF EXISTS pk_filter_sensitive_expressions;
CREATE TABLE pk_filter_sensitive_expressions (k UInt64, amount UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;
INSERT INTO pk_filter_sensitive_expressions
SELECT number, number + 1 FROM numbers(256)
SETTINGS max_insert_threads = 1, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

-- Pure expressions, including lambdas, must still use the exact interior ranges.
SELECT 'pure_expression', countIf(explain LIKE '%Primary key proven filter%') > 0
FROM (EXPLAIN description = 1
    SELECT sum(amount + 1) FROM pk_filter_sensitive_expressions WHERE k >= 13 AND k < 243);
SELECT 'pure_lambda', countIf(explain LIKE '%Primary key proven filter%') > 0
FROM (EXPLAIN description = 1
    SELECT sum(arraySum(arrayMap(x -> x + 1, [amount])))
    FROM pk_filter_sensitive_expressions WHERE k >= 13 AND k < 243);

-- `sleep` is deterministic but has observable side effects. Zero seconds avoids a timing assertion.
SELECT 'direct_side_effect', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1
    SELECT sum(amount + sleep(0)) FROM pk_filter_sensitive_expressions WHERE k >= 13 AND k < 243);

-- `ignore` returns constant zero at execution, while keeping `sleep` dependent on the lambda argument.
-- This prevents hoisting the side effect out of the lambda. Cover both uncaptured and captured bodies.
SELECT 'lambda_side_effect', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1
    SELECT sum(arraySum(arrayMap(x -> x + sleep(ignore(x)), [amount])))
    FROM pk_filter_sensitive_expressions WHERE k >= 13 AND k < 243);
SELECT 'captured_lambda_side_effect', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1
    SELECT sum(arraySum(arrayMap(x -> x + amount + sleep(ignore(x)), [toUInt64(1)])))
    FROM pk_filter_sensitive_expressions WHERE k >= 13 AND k < 243);
SELECT 'nested_lambda_side_effect', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1
    SELECT sum(arraySum(arrayMap(x -> arraySum(arrayMap(y -> y + sleep(ignore(y)), [x])), [amount])))
    FROM pk_filter_sensitive_expressions WHERE k >= 13 AND k < 243);

SELECT 'enabled',
    sum(amount + 1),
    sum(arraySum(arrayMap(x -> x + 1, [amount]))),
    sum(amount + sleep(0)),
    sum(arraySum(arrayMap(x -> x + sleep(ignore(x)), [amount]))),
    sum(arraySum(arrayMap(x -> x + amount + sleep(ignore(x)), [toUInt64(1)]))),
    sum(arraySum(arrayMap(x -> arraySum(arrayMap(y -> y + sleep(ignore(y)), [x])), [amount])))
FROM pk_filter_sensitive_expressions WHERE k >= 13 AND k < 243;
SELECT 'disabled',
    sum(amount + 1),
    sum(arraySum(arrayMap(x -> x + 1, [amount]))),
    sum(amount + sleep(0)),
    sum(arraySum(arrayMap(x -> x + sleep(ignore(x)), [amount]))),
    sum(arraySum(arrayMap(x -> x + amount + sleep(ignore(x)), [toUInt64(1)]))),
    sum(arraySum(arrayMap(x -> arraySum(arrayMap(y -> y + sleep(ignore(y)), [x])), [amount])))
FROM pk_filter_sensitive_expressions WHERE k >= 13 AND k < 243
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0;

DROP TABLE pk_filter_sensitive_expressions;
