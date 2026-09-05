SET max_threads = 1, max_insert_threads = 1;

CREATE TABLE dictionary_stochastic_regression
(
    part UInt8,
    id UInt64,
    k LowCardinality(String),
    target Float64,
    feature Float64
)
ENGINE = MergeTree
PARTITION BY part
ORDER BY id
SETTINGS index_granularity = 1, index_granularity_bytes = 0,
    min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0, min_level_for_wide_part = 0;

-- Neither dictionary contains a complete default minibatch of 15 samples.
-- Training must continue across the switch, rather than merging two untrained models.
INSERT INTO dictionary_stochastic_regression SELECT 0, number, 'shared', 1, 1 FROM numbers(7);
INSERT INTO dictionary_stochastic_regression SELECT 1, number + 7, 'shared', 1, 1 FROM numbers(8);

SELECT throwIf(count() != 2 OR countIf(part_type = 'Wide') != 2
    OR countIf(rows = 7) != 1 OR countIf(rows = 8) != 1,
    'Expected one seven-row and one eight-row Wide part')
FROM system.parts
WHERE database = currentDatabase() AND table = 'dictionary_stochastic_regression' AND active
FORMAT Null;

CREATE VIEW dictionary_stochastic_regression_input AS
SELECT k, target, feature FROM dictionary_stochastic_regression ORDER BY id;

SET max_threads = 1, enable_parallel_replicas = 0, serialize_query_plan = 0,
    max_streams_for_merge_tree_reading = 1, max_block_size = 1, preferred_block_size_bytes = 0,
    merge_tree_use_deserialization_prefixes_cache = 1,
    optimize_read_in_order = 1, query_plan_remove_redundant_sorting = 0,
    optimize_aggregation_in_order = 0, enable_adaptive_aggregator = 0,
    allow_aggregate_partitions_independently = 0, force_aggregate_partitions_independently = 0,
    enable_lazy_columns_replication = 0, collect_hash_table_stats_during_aggregation = 0,
    compile_aggregate_expressions = 0, max_rows_to_group_by = 0,
    group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0,
    max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0,
    use_query_cache = 0, log_queries = 1, log_profile_events = 1, log_queries_probability = 1,
    log_queries_min_query_duration_ms = 0, log_queries_min_type = 'QUERY_FINISH';

SELECT 'String linear', CAST(k AS String) AS key,
    arrayExists(w -> w != 0, stochasticLinearRegression(target, feature)) AS trained
FROM dictionary_stochastic_regression_input GROUP BY key;

SELECT 'String logistic', CAST(k AS String) AS key,
    arrayExists(w -> w != 0, stochasticLogisticRegression(target, feature)) AS trained
FROM dictionary_stochastic_regression_input GROUP BY key;

-- Separate queries ensure one function's metadata cannot mask the other's omission.
SELECT 'linear single-level', k,
    arrayExists(w -> w != 0, stochasticLinearRegression(target, feature)) AS trained
FROM dictionary_stochastic_regression_input GROUP BY k
SETTINGS log_comment = '05092_dictionary_stochastic_regression/linear-single';

SELECT 'linear two-level', k,
    arrayExists(w -> w != 0, stochasticLinearRegression(target, feature)) AS trained
FROM dictionary_stochastic_regression_input GROUP BY k
SETTINGS group_by_two_level_threshold = 1, log_comment = '05092_dictionary_stochastic_regression/linear-two';

SELECT 'logistic single-level', k,
    arrayExists(w -> w != 0, stochasticLogisticRegression(target, feature)) AS trained
FROM dictionary_stochastic_regression_input GROUP BY k
SETTINGS log_comment = '05092_dictionary_stochastic_regression/logistic-single';

SELECT 'logistic two-level', k,
    arrayExists(w -> w != 0, stochasticLogisticRegression(target, feature)) AS trained
FROM dictionary_stochastic_regression_input GROUP BY k
SETTINGS group_by_two_level_threshold = 1, log_comment = '05092_dictionary_stochastic_regression/logistic-two';

-- Confirm that the fixture still exercises sharding for an order-independent aggregate.
SELECT 'sum control', k, sum(target)
FROM dictionary_stochastic_regression_input GROUP BY k
SETTINGS log_comment = '05092_dictionary_stochastic_regression/control';

SYSTEM FLUSH LOGS query_log;

SELECT
    substring(log_comment, length('05092_dictionary_stochastic_regression/') + 1) AS mode,
    ProfileEvents['AggregationSingleLowCardinalityDictionarySwitches'] > 0 AS sharded
FROM system.query_log
WHERE current_database = currentDatabase()
    AND startsWith(log_comment, '05092_dictionary_stochastic_regression/')
    AND type = 'QueryFinish'
ORDER BY mode;

DROP VIEW dictionary_stochastic_regression_input;
DROP TABLE dictionary_stochastic_regression;
