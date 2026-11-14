-- The optimization requires a local plan; the distributed-plan suite enables `serialize_query_plan`.
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
    max_threads = 2,
    max_block_size = 7,
    query_plan_optimize_primary_key_filter_elimination = 1;

DROP TABLE IF EXISTS pk_filter_elimination;
CREATE TABLE pk_filter_elimination
(
    tenant UInt32,
    ts UInt64,
    bucket UInt8,
    amount UInt64
)
ENGINE = MergeTree ORDER BY (tenant, ts)
SETTINGS index_granularity = 8, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

-- Keep two overlapping parts. The filter below selects both exact interior marks
-- and partial boundary marks in each part. Blocks are smaller than one mark.
SYSTEM STOP MERGES pk_filter_elimination;
INSERT INTO pk_filter_elimination
SELECT intDiv(number, 64), number % 64, (number % 64) % 3, number + 1
FROM numbers(192)
SETTINGS max_insert_threads = 1, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;
INSERT INTO pk_filter_elimination
SELECT intDiv(number, 64), number % 64, (number % 64) % 3, number + 1001
FROM numbers(192)
SETTINGS max_insert_threads = 1, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

SELECT 'disabled_plan', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1
    SELECT bucket, sum(amount) FROM pk_filter_elimination
    WHERE tenant = 1 AND ts >= 13 AND ts < 51 GROUP BY bucket
    SETTINGS query_plan_optimize_primary_key_filter_elimination = 0);
SELECT 'enabled_plan', countIf(explain LIKE '%Primary key proven filter%') > 0
FROM (EXPLAIN description = 1
    SELECT bucket, sum(amount) FROM pk_filter_elimination
    WHERE tenant = 1 AND ts >= 13 AND ts < 51 GROUP BY bucket);

SELECT 'where_disabled', bucket, sum(amount) FROM pk_filter_elimination
WHERE tenant = 1 AND ts >= 13 AND ts < 51
GROUP BY bucket ORDER BY bucket
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0;
SELECT 'where_enabled', bucket, sum(amount) FROM pk_filter_elimination
WHERE tenant = 1 AND ts >= 13 AND ts < 51
GROUP BY bucket ORDER BY bucket;

-- A serialized plan must retain the filter and return the same aggregate results.
SELECT 'serialized_plan', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1
    SELECT bucket, sum(amount) FROM pk_filter_elimination
    WHERE tenant = 1 AND ts >= 13 AND ts < 51 GROUP BY bucket
    SETTINGS serialize_query_plan = 1);
SELECT 'serialized', bucket, sum(amount) FROM pk_filter_elimination
WHERE tenant = 1 AND ts >= 13 AND ts < 51
GROUP BY bucket ORDER BY bucket SETTINGS serialize_query_plan = 1;

-- Automatic and explicit `PREWHERE` must produce the same proof and results.
SELECT 'automatic_prewhere_plan', countIf(explain LIKE '%Primary key proven filter%') > 0
FROM (EXPLAIN description = 1
    SELECT bucket, sum(amount) FROM pk_filter_elimination
    WHERE tenant = 1 AND ts >= 13 AND ts < 51 GROUP BY bucket
    SETTINGS optimize_move_to_prewhere = 1);
SELECT 'automatic_prewhere', bucket, sum(amount) FROM pk_filter_elimination
WHERE tenant = 1 AND ts >= 13 AND ts < 51
GROUP BY bucket ORDER BY bucket SETTINGS optimize_move_to_prewhere = 1;
SELECT 'explicit_prewhere', sum(amount) FROM pk_filter_elimination
PREWHERE tenant = 1 AND ts >= 13 AND ts < 51;
SELECT 'prewhere_and_where', sum(amount) FROM pk_filter_elimination
PREWHERE tenant = 1 WHERE ts >= 13 AND ts < 51;

-- Reversing the comparison must not reverse an inclusive or exclusive boundary.
SELECT 'reversed', sum(amount) FROM pk_filter_elimination
WHERE 1 = tenant AND 13 <= ts AND 51 > ts;
SELECT 'inclusive_upper', sum(amount) FROM pk_filter_elimination
WHERE tenant = 1 AND ts > 12 AND ts <= 50;
SELECT 'aggregate_expression', sum(amount * (bucket + 1)) FROM pk_filter_elimination
WHERE tenant = 1 AND ts >= 13 AND ts < 51;
-- Expression steps can pass columns through implicitly. A declined rewrite must
-- preserve those headers just as a successfully rewritten plan does.
SELECT 'unmerged_disabled', sum(amount) FROM pk_filter_elimination
PREWHERE tenant = 1 AND ts >= 13 AND ts < 51
SETTINGS query_plan_merge_expressions = 0, query_plan_remove_unused_columns = 0,
    query_plan_optimize_primary_key_filter_elimination = 0;
SELECT 'unmerged_enabled', sum(amount) FROM pk_filter_elimination
PREWHERE tenant = 1 AND ts >= 13 AND ts < 51
SETTINGS query_plan_merge_expressions = 0, query_plan_remove_unused_columns = 0;
SELECT 'subquery_alias', sum(renamed_amount)
FROM (SELECT tenant AS tenant_alias, ts AS time_alias, amount AS renamed_amount FROM pk_filter_elimination)
WHERE tenant_alias = 1 AND time_alias >= 13 AND time_alias < 51
SETTINGS query_plan_merge_expressions = 0;

-- Required key columns must survive the exact branch even though their predicate
-- no longer needs evaluation. Keeping every key also removes the read-saving opportunity.
SELECT 'retained_key', min(ts), max(ts), sum(ts), sum(amount) FROM pk_filter_elimination
WHERE tenant = 1 AND ts >= 13 AND ts < 51;
SELECT 'retained_all_keys', tenant, sum(ts), sum(amount) FROM pk_filter_elimination
WHERE tenant = 1 AND ts >= 13 AND ts < 51 GROUP BY tenant;
SELECT 'retained_all_keys_plan', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT tenant, sum(ts), sum(amount) FROM pk_filter_elimination
    WHERE tenant = 1 AND ts >= 13 AND ts < 51 GROUP BY tenant);

-- Splitting into a `Union` must not prevent the later aggregation-in-order pass.
SELECT 'aggregation_in_order_plan', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT tenant, sum(amount) FROM pk_filter_elimination
    WHERE tenant <= 1 AND ts >= 13 AND ts < 51 GROUP BY tenant
    SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1);
SELECT 'aggregation_in_order_pipeline', countIf(explain LIKE '%AggregatingInOrder%') > 0
FROM (EXPLAIN PIPELINE SELECT tenant, sum(amount) FROM pk_filter_elimination
    WHERE tenant <= 1 AND ts >= 13 AND ts < 51 GROUP BY tenant
    SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1);
SELECT 'aggregation_in_order', tenant, sum(amount) FROM pk_filter_elimination
WHERE tenant <= 1 AND ts >= 13 AND ts < 51 GROUP BY tenant ORDER BY tenant
SETTINGS optimize_aggregation_in_order = 1, optimize_read_in_order = 1;

-- A column missing in old parts can require a key column through its default expression.
ALTER TABLE pk_filter_elimination ADD COLUMN derived UInt64 DEFAULT ts * 3 + amount;
SELECT 'default_dependency_plan', countIf(explain LIKE '%Primary key proven filter%') > 0
FROM (EXPLAIN description = 1 SELECT sum(derived) FROM pk_filter_elimination
    WHERE tenant = 1 AND ts >= 13 AND ts < 51);
SELECT 'default_dependency', sum(derived) FROM pk_filter_elimination
WHERE tenant = 1 AND ts >= 13 AND ts < 51;

-- The last mark, an empty selection, and an interval with no exact marks.
SELECT 'all_rows', sum(amount) FROM pk_filter_elimination
WHERE tenant >= 0 AND tenant <= 2 AND ts >= 0 AND ts <= 63;
SELECT 'empty', sum(amount) FROM pk_filter_elimination
WHERE tenant = 1 AND ts >= 13 AND ts < 13;
SELECT 'boundary_only_plan', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT sum(amount) FROM pk_filter_elimination
    WHERE tenant = 1 AND ts >= 13 AND ts < 15);
SELECT 'boundary_only', sum(amount) FROM pk_filter_elimination
WHERE tenant = 1 AND ts >= 13 AND ts < 15;

-- The selected exact and boundary ranges each fit below this limit, but their
-- combined read does not. Splitting the plan must not bypass the original limit.
SELECT sum(amount) FROM pk_filter_elimination
WHERE tenant = 1 AND ts >= 13 AND ts < 51
SETTINGS max_rows_to_read = 70, read_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS }
SELECT sum(amount) FROM pk_filter_elimination
WHERE tenant = 1 AND ts >= 13 AND ts < 51
SETTINGS max_rows_to_read = 70, read_overflow_mode = 'throw',
    query_plan_optimize_primary_key_filter_elimination = 0; -- { serverError TOO_MANY_ROWS }

-- A non-key residual cannot be discarded using the proof of just the key atoms.
SELECT 'residual_plan', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT sum(amount) FROM pk_filter_elimination
    WHERE tenant = 1 AND ts >= 13 AND ts < 51 AND amount > 1000);
SELECT 'residual', sum(amount) FROM pk_filter_elimination
WHERE tenant = 1 AND ts >= 13 AND ts < 51 AND amount > 1000;

-- The bounded implementation declines disjunctions and lossy literal conversions.
-- Disable comparison normalization for the literal checks so it cannot first
-- replace them with equivalent integer predicates or remove a tautology.
SELECT 'disjunction_plan', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT sum(amount) FROM pk_filter_elimination
    WHERE tenant = 1 AND ((ts >= 13 AND ts < 20) OR (ts >= 40 AND ts < 51)));
SELECT 'disjunction', sum(amount) FROM pk_filter_elimination
WHERE tenant = 1 AND ((ts >= 13 AND ts < 20) OR (ts >= 40 AND ts < 51));
SELECT 'fractional_literal_plan', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT sum(amount) FROM pk_filter_elimination
    WHERE tenant = 1 AND ts >= 13.5 AND ts < 51
    SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0);
SELECT 'fractional_literal', sum(amount) FROM pk_filter_elimination
WHERE tenant = 1 AND ts >= 13.5 AND ts < 51
SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0;
SELECT 'negative_literal_plan', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT sum(amount) FROM pk_filter_elimination
    WHERE tenant = 1 AND ts >= -1
    SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0);
SELECT 'negative_literal', sum(amount) FROM pk_filter_elimination
WHERE tenant = 1 AND ts >= -1
SETTINGS optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0;

DROP TABLE IF EXISTS pk_filter_final;
CREATE TABLE pk_filter_final (tenant UInt32, ts UInt64, amount UInt64)
ENGINE = ReplacingMergeTree(amount) ORDER BY (tenant, ts)
SETTINGS index_granularity = 8, index_granularity_bytes = 0;
INSERT INTO pk_filter_final SELECT tenant, ts, amount FROM pk_filter_elimination;
SELECT 'final_plan', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT sum(amount) FROM pk_filter_final FINAL
    WHERE tenant = 1 AND ts >= 13 AND ts < 51);
SELECT 'final', sum(amount) FROM pk_filter_final FINAL
WHERE tenant = 1 AND ts >= 13 AND ts < 51;

-- Preserve the pre-existing count optimization and its read-limit deferral:
-- only uncertain marks need row reads, so this count fits the limit above.
SELECT 'count_plan', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT count() FROM pk_filter_elimination
    WHERE tenant = 1 AND ts >= 13 AND ts < 51
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1,
        optimize_trivial_count_query = 1, max_rows_to_read = 70);
SELECT 'count', count() FROM pk_filter_elimination
WHERE tenant = 1 AND ts >= 13 AND ts < 51
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1,
    optimize_trivial_count_query = 1, max_rows_to_read = 70;

DROP TABLE IF EXISTS pk_filter_datetime;
CREATE TABLE pk_filter_datetime (d Date, ts DateTime('UTC'), amount UInt64)
ENGINE = MergeTree ORDER BY (d, ts)
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO pk_filter_datetime
SELECT toDate('2026-10-01'), toDateTime('2026-10-01 00:00:00', 'UTC') + number, number FROM numbers(128)
SETTINGS max_insert_threads = 1, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;
SELECT 'datetime_plan', countIf(explain LIKE '%Primary key proven filter%') > 0
FROM (EXPLAIN description = 1 SELECT sum(amount) FROM pk_filter_datetime
    WHERE d = toDate('2026-10-01')
        AND ts >= toDateTime('2026-10-01 00:00:13', 'UTC')
        AND ts < toDateTime('2026-10-01 00:01:55', 'UTC'));
SELECT 'datetime', sum(amount) FROM pk_filter_datetime
WHERE d = toDate('2026-10-01')
    AND ts >= toDateTime('2026-10-01 00:00:13', 'UTC')
    AND ts < toDateTime('2026-10-01 00:01:55', 'UTC');

DROP TABLE IF EXISTS pk_filter_datetime64;
CREATE TABLE pk_filter_datetime64 (ts DateTime64(3, 'UTC'), amount UInt64)
ENGINE = MergeTree ORDER BY ts
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO pk_filter_datetime64
SELECT toDateTime64('2026-10-01 00:00:00.000', 3, 'UTC') + toIntervalMillisecond(number), number FROM numbers(128)
SETTINGS max_insert_threads = 1, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;
SELECT 'datetime64_plan', countIf(explain LIKE '%Primary key proven filter%') > 0
FROM (EXPLAIN description = 1 SELECT sum(amount) FROM pk_filter_datetime64
    WHERE ts >= toDateTime64('2026-10-01 00:00:00.013', 3, 'UTC')
        AND ts < toDateTime64('2026-10-01 00:00:00.115', 3, 'UTC'));
SELECT 'datetime64', sum(amount) FROM pk_filter_datetime64
WHERE ts >= toDateTime64('2026-10-01 00:00:00.013', 3, 'UTC')
    AND ts < toDateTime64('2026-10-01 00:00:00.115', 3, 'UTC');

DROP TABLE IF EXISTS pk_filter_nullable;
CREATE TABLE pk_filter_nullable (k Nullable(UInt64), amount UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS allow_nullable_key = 1, index_granularity = 8, index_granularity_bytes = 0;
INSERT INTO pk_filter_nullable SELECT if(number % 8 = 0, NULL, number), number FROM numbers(64);
SELECT 'nullable_plan', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT sum(amount) FROM pk_filter_nullable WHERE k >= 13 AND k < 51);
SELECT 'nullable', sum(amount) FROM pk_filter_nullable WHERE k >= 13 AND k < 51;

DROP TABLE IF EXISTS pk_filter_float;
CREATE TABLE pk_filter_float (k Float64, amount UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0;
INSERT INTO pk_filter_float SELECT number, number FROM numbers(64);
SELECT 'float_plan', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT sum(amount) FROM pk_filter_float WHERE k >= 13 AND k < 51);
SELECT 'float', sum(amount) FROM pk_filter_float WHERE k >= 13 AND k < 51;

DROP TABLE IF EXISTS pk_filter_expression;
CREATE TABLE pk_filter_expression (k UInt64, amount UInt64)
ENGINE = MergeTree ORDER BY intDiv(k, 8)
SETTINGS index_granularity = 8, index_granularity_bytes = 0;
INSERT INTO pk_filter_expression SELECT number, number FROM numbers(64);
SELECT 'transformed_key_plan', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT sum(amount) FROM pk_filter_expression WHERE k >= 13 AND k < 51);
SELECT 'transformed_key', sum(amount) FROM pk_filter_expression WHERE k >= 13 AND k < 51;

DROP TABLE pk_filter_expression;
DROP TABLE pk_filter_float;
DROP TABLE pk_filter_nullable;
DROP TABLE pk_filter_datetime64;
DROP TABLE pk_filter_datetime;
DROP TABLE pk_filter_final;
DROP TABLE pk_filter_elimination;
