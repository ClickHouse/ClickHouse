
DROP TABLE IF EXISTS test_col_stats_agg_filter;

CREATE TABLE test_col_stats_agg_filter (
    id UInt64,
    value Int32,
    p Int32
) ENGINE = MergeTree
PARTITION BY p
ORDER BY id
SETTINGS auto_statistics_types = 'basic';

SET materialize_statistics_on_insert = 1;

-- p = 1: values {50, 200}; p = 2: values {25, 400}
INSERT INTO test_col_stats_agg_filter VALUES (1, 50, 1), (2, 200, 1);
INSERT INTO test_col_stats_agg_filter VALUES (3, 25, 2), (4, 400, 2);

-- Suppress CI setting randomization to ensure deterministic test behavior.
SET explain_query_plan_default = 'legacy';
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;
SET use_statistics_for_min_max_aggregation = 1;
SET optimize_time_filter_with_preimage = 1;

-- The shortcut replaces the read with a prepared source, so there is no residual
-- FilterStep: the filter must be enforced by the shortcut itself. With pruning
-- analysis disabled the results must still be the filtered extrema (50, 200),
-- not the whole-table ones (25, 400).
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_agg_filter WHERE p = 1 SETTINGS use_partition_pruning = 0) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value), max(value) FROM test_col_stats_agg_filter WHERE p = 1 SETTINGS use_partition_pruning = 0;

-- Same query with default settings.
SELECT min(value), max(value) FROM test_col_stats_agg_filter WHERE p = 1;

-- Pruning-related settings disabled one by one.
SELECT min(value), max(value) FROM test_col_stats_agg_filter WHERE p = 1 SETTINGS use_skip_indexes = 0;
SELECT min(value), max(value) FROM test_col_stats_agg_filter WHERE p = 1 SETTINGS use_constant_folding_in_index_analysis = 0;

-- Non-monotonic predicate on the partition key: index analysis cannot enforce it
-- even with default settings, exact evaluation must.
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_agg_filter WHERE p % 2 = 0) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value), max(value) FROM test_col_stats_agg_filter WHERE p % 2 = 0;
SELECT min(value), max(value) FROM test_col_stats_agg_filter WHERE p % 2 = 1;

-- GROUP BY partition key with a filter and pruning disabled.
SELECT trimLeft(explain) FROM (EXPLAIN SELECT p, min(value), max(value) FROM test_col_stats_agg_filter WHERE p != 1 GROUP BY p ORDER BY p SETTINGS use_partition_pruning = 0) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT p, min(value), max(value) FROM test_col_stats_agg_filter WHERE p != 1 GROUP BY p ORDER BY p SETTINGS use_partition_pruning = 0;

-- Filter matching nothing.
SELECT p, min(value), max(value) FROM test_col_stats_agg_filter WHERE p = 99 GROUP BY p ORDER BY p SETTINGS use_partition_pruning = 0;
SELECT min(value), max(value) FROM test_col_stats_agg_filter WHERE p = 99 SETTINGS use_partition_pruning = 0;

-- Filter on a virtual column with pruning disabled.
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_agg_filter WHERE _partition_id = '1' SETTINGS use_partition_pruning = 0) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value), max(value) FROM test_col_stats_agg_filter WHERE _partition_id = '1' SETTINGS use_partition_pruning = 0;

-- Filter referencing a non-partition column: the shortcut must not be used.
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_agg_filter WHERE id > 1 SETTINGS use_partition_pruning = 0) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT min(value), max(value) FROM test_col_stats_agg_filter WHERE id > 1 SETTINGS use_partition_pruning = 0;

-- Mixed partition and non-partition conjuncts: the shortcut must not be used
-- (no residual FilterStep exists to enforce the non-partition conjunct).
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_agg_filter WHERE p = 1 AND id > 1 SETTINGS use_partition_pruning = 0) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT min(value), max(value) FROM test_col_stats_agg_filter WHERE p = 1 AND id > 1 SETTINGS use_partition_pruning = 0;

DROP TABLE test_col_stats_agg_filter;

-- Partition key with an expression.
DROP TABLE IF EXISTS test_col_stats_agg_expr;

CREATE TABLE test_col_stats_agg_expr (
    id UInt64,
    value Int32,
    d Date
) ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY id
SETTINGS auto_statistics_types = 'basic';

INSERT INTO test_col_stats_agg_expr VALUES (1, 10, '2024-01-05'), (2, 100, '2024-01-20');
INSERT INTO test_col_stats_agg_expr VALUES (3, 20, '2024-02-05'), (4, 200, '2024-02-20');

-- Equality on a Date partition expression is rewritten by the analyzer into a
-- range over the physical column (optimize_time_filter_with_preimage), which is
-- not part-level resolvable: the shortcut declines and the normal read answers.
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_agg_expr WHERE toYYYYMM(d) = 202401 SETTINGS use_partition_pruning = 0) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT min(value), max(value) FROM test_col_stats_agg_expr WHERE toYYYYMM(d) = 202401 SETTINGS use_partition_pruning = 0;

-- With the preimage rewrite disabled, the predicate stays in partition-expression
-- form and is evaluated exactly on each part's partition value.
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_agg_expr WHERE toYYYYMM(d) = 202401 SETTINGS optimize_time_filter_with_preimage = 0) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value), max(value) FROM test_col_stats_agg_expr WHERE toYYYYMM(d) = 202401 SETTINGS optimize_time_filter_with_preimage = 0;

-- Non-monotonic function over the partition expression is not rewritable: the
-- shortcut applies and enforces the filter exactly, with or without pruning.
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_agg_expr WHERE toYYYYMM(d) % 2 = 0) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value), max(value) FROM test_col_stats_agg_expr WHERE toYYYYMM(d) % 2 = 0;
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_agg_expr WHERE toYYYYMM(d) % 2 = 0 SETTINGS use_partition_pruning = 0) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value), max(value) FROM test_col_stats_agg_expr WHERE toYYYYMM(d) % 2 = 0 SETTINGS use_partition_pruning = 0;

DROP TABLE test_col_stats_agg_expr;
