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

DROP TABLE IF EXISTS pk_filter_reader_defaults;
DROP TABLE IF EXISTS pk_filter_reader_default_results;
CREATE TABLE pk_filter_reader_defaults (k UInt64, amount UInt64)
ENGINE = MergeTree PARTITION BY intDiv(k, 256) ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES pk_filter_reader_defaults;
INSERT INTO pk_filter_reader_defaults
SELECT number, number + 1 FROM numbers(256)
SETTINGS max_insert_threads = 1, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

-- These columns are absent from the existing part, so their defaults run during reading.
ALTER TABLE pk_filter_reader_defaults
    ADD COLUMN derived UInt64 DEFAULT amount + sleep(0),
    ADD COLUMN block_ordinal UInt64 DEFAULT rowNumberInBlock(),
    ADD COLUMN indirect UInt64 DEFAULT derived + 1,
    ADD COLUMN materialized UInt64 MATERIALIZED indirect + 1,
    ADD COLUMN lambda_default UInt64 DEFAULT arraySum(arrayMap(x -> x + sleep(ignore(x)), [amount])),
    ADD COLUMN captured_default UInt64 DEFAULT arraySum(arrayMap(x -> x + amount + sleep(ignore(x)), [toUInt64(1)])),
    ADD COLUMN stamp DateTime64(9) DEFAULT now64(9),
    ADD COLUMN lambda_stamp UInt64 DEFAULT arraySum(arrayMap(x -> x + toUnixTimestamp64Nano(now64(9)), [amount])),
    ADD COLUMN pure UInt64 DEFAULT k * 3 + amount;

SELECT 'default_side_effect', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT sum(derived) FROM pk_filter_reader_defaults WHERE k >= 13 AND k < 243);
SELECT 'default_stateful', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT sum(block_ordinal) FROM pk_filter_reader_defaults WHERE k >= 13 AND k < 243);
SELECT 'materialized_transitive', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT sum(materialized) FROM pk_filter_reader_defaults WHERE k >= 13 AND k < 243);
SELECT 'default_lambda', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT sum(lambda_default) FROM pk_filter_reader_defaults WHERE k >= 13 AND k < 243);
SELECT 'default_captured_lambda', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT sum(captured_default) FROM pk_filter_reader_defaults WHERE k >= 13 AND k < 243);
SELECT 'default_query_constant', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT uniqExact(stamp) FROM pk_filter_reader_defaults WHERE k >= 13 AND k < 243);
SELECT 'default_lambda_query_constant', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT uniqExact(lambda_stamp - amount) FROM pk_filter_reader_defaults WHERE k >= 13 AND k < 243);

-- Unused sensitive defaults and pure reader defaults must not disable eligible reads.
SELECT 'unused_defaults', countIf(explain LIKE '%Primary key proven filter%') > 0
FROM (EXPLAIN description = 1 SELECT sum(amount) FROM pk_filter_reader_defaults WHERE k >= 13 AND k < 243);
SELECT 'pure_default', countIf(explain LIKE '%Primary key proven filter%') > 0
FROM (EXPLAIN description = 1 SELECT sum(pure) FROM pk_filter_reader_defaults WHERE k >= 13 AND k < 243);

CREATE TABLE pk_filter_reader_default_results
    (ordinal UInt64, derived UInt64, materialized UInt64, lambda_default UInt64, captured_default UInt64, pure UInt64)
ENGINE = Memory;
INSERT INTO pk_filter_reader_default_results
SELECT sum(block_ordinal), sum(derived), sum(materialized), sum(lambda_default), sum(captured_default), sum(pure)
FROM pk_filter_reader_defaults WHERE k >= 13 AND k < 243
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0;
INSERT INTO pk_filter_reader_default_results
SELECT sum(block_ordinal), sum(derived), sum(materialized), sum(lambda_default), sum(captured_default), sum(pure)
FROM pk_filter_reader_defaults WHERE k >= 13 AND k < 243;
SELECT 'where_results_equal', uniqExact(tuple(*)) = 1 FROM pk_filter_reader_default_results;

TRUNCATE TABLE pk_filter_reader_default_results;
SET optimize_move_to_prewhere = 1;
SELECT 'prewhere_default', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT sum(derived) FROM pk_filter_reader_defaults WHERE k >= 13 AND k < 243);
INSERT INTO pk_filter_reader_default_results
SELECT sum(block_ordinal), sum(derived), sum(materialized), sum(lambda_default), sum(captured_default), sum(pure)
FROM pk_filter_reader_defaults WHERE k >= 13 AND k < 243
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0;
INSERT INTO pk_filter_reader_default_results
SELECT sum(block_ordinal), sum(derived), sum(materialized), sum(lambda_default), sum(captured_default), sum(pure)
FROM pk_filter_reader_defaults WHERE k >= 13 AND k < 243;
SELECT 'prewhere_results_equal', uniqExact(tuple(*)) = 1 FROM pk_filter_reader_default_results;
SET optimize_move_to_prewhere = 0;

-- A newer part stores the defaults. A later pure default must stop at that stored dependency,
-- while the same dependency still needs evaluation in the old part.
-- Partition pruning excludes the old part even if primary-key analysis keeps its final granule.
INSERT INTO pk_filter_reader_defaults (k, amount)
SELECT number, number + 1 FROM numbers(256, 256)
SETTINGS max_insert_threads = 1, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;
ALTER TABLE pk_filter_reader_defaults ADD COLUMN later UInt64 DEFAULT derived + 3;
SELECT 'stored_default', countIf(explain LIKE '%Primary key proven filter%') > 0
FROM (EXPLAIN description = 1 SELECT sum(derived) FROM pk_filter_reader_defaults WHERE k >= 269 AND k < 499);
SELECT 'stored_dependency', countIf(explain LIKE '%Primary key proven filter%') > 0
FROM (EXPLAIN description = 1 SELECT sum(later) FROM pk_filter_reader_defaults WHERE k >= 269 AND k < 499);
SELECT 'missing_dependency', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT sum(later) FROM pk_filter_reader_defaults WHERE k >= 13 AND k < 243);
SELECT 'mixed_parts', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT sum(later) FROM pk_filter_reader_defaults WHERE k >= 13 AND k < 499);
SELECT 'stored_dependency_enabled', sum(later) FROM pk_filter_reader_defaults WHERE k >= 269 AND k < 499;
SELECT 'stored_dependency_disabled', sum(later) FROM pk_filter_reader_defaults WHERE k >= 269 AND k < 499
SETTINGS query_plan_optimize_primary_key_filter_elimination = 0;

-- The absence check must also work when the part uses the compact format.
DROP TABLE IF EXISTS pk_filter_compact_defaults;
CREATE TABLE pk_filter_compact_defaults (k UInt64, amount UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 10485760,
    min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000,
    add_minmax_index_for_numeric_columns = 0;
INSERT INTO pk_filter_compact_defaults SELECT number, number + 1 FROM numbers(256)
SETTINGS max_insert_threads = 1, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;
SELECT 'compact_part', count() = 1 AND any(part_type) = 'Compact' FROM system.parts
WHERE database = currentDatabase() AND table = 'pk_filter_compact_defaults' AND active;
ALTER TABLE pk_filter_compact_defaults
    ADD COLUMN derived UInt64 DEFAULT amount + sleep(0),
    ADD COLUMN pure UInt64 DEFAULT amount + 1;
SELECT 'compact_sensitive_default', countIf(explain LIKE '%Primary key proven filter%') = 0
FROM (EXPLAIN description = 1 SELECT sum(derived) FROM pk_filter_compact_defaults WHERE k >= 13 AND k < 243);
SELECT 'compact_pure_default', countIf(explain LIKE '%Primary key proven filter%') > 0
FROM (EXPLAIN description = 1 SELECT sum(pure) FROM pk_filter_compact_defaults WHERE k >= 13 AND k < 243);

DROP TABLE pk_filter_compact_defaults;
DROP TABLE pk_filter_reader_default_results;
DROP TABLE pk_filter_reader_defaults;
