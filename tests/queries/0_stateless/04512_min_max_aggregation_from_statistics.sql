-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test checks EXPLAIN of plans with reading steps replaced by prepared sources.

DROP TABLE IF EXISTS t_min_max_from_stats;

CREATE TABLE t_min_max_from_stats
(
    key UInt64,
    value Int32,
    date Date,
    nullable_value Nullable(Int32),
    str String
)
ENGINE = MergeTree ORDER BY (key, date)
SETTINGS auto_statistics_types = 'minmax', materialize_statistics_on_merge = 1;

SET mutations_sync = 2;
SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET use_statistics_for_min_max_aggregation = 1;
SET materialize_statistics_on_insert = 0;
SET optimize_arithmetic_operations_in_aggregate_functions = 1;
-- Pin every setting the optimization's eligibility depends on: the test runner randomizes
-- optimize_aggregation_in_order, and aggregation-in-order (like aggregate_functions_null_for_empty)
-- disables the projection optimization entirely, turning the EXPLAIN checks below into 0.
SET enable_analyzer = 1;
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0;
SET aggregate_functions_null_for_empty = 0;

INSERT INTO t_min_max_from_stats
    SELECT number, toInt32(number % 1000) - 500, toDate('2020-01-01') + number % 365, if(number % 10 = 0, NULL, toInt32(number)), toString(number)
    FROM numbers(100000);

SELECT 'no statistics yet';
SELECT min(date), max(date), min(value), max(value), count() FROM t_min_max_from_stats;
SELECT count() FROM (EXPLAIN SELECT min(date), max(date) FROM t_min_max_from_stats) WHERE explain LIKE '%_statistics_min_max_projection%';

SELECT 'statistics are materialized by the merge';
OPTIMIZE TABLE t_min_max_from_stats FINAL;
SELECT min(date), max(date), min(value), max(value), count() FROM t_min_max_from_stats;
SELECT count() FROM (EXPLAIN SELECT min(date), max(date), min(value), max(value), count() FROM t_min_max_from_stats) WHERE explain LIKE '%_statistics_min_max_projection%';

SELECT 'a part without statistics is read and combined with the statistics of other parts';
SYSTEM STOP MERGES t_min_max_from_stats;
INSERT INTO t_min_max_from_stats VALUES (1000000, -600, '2021-05-05', 42, 'foo');
SELECT min(date), max(date), min(value), max(value), count() FROM t_min_max_from_stats;
SELECT count() FROM (EXPLAIN SELECT min(date), max(date) FROM t_min_max_from_stats) WHERE explain LIKE '%_statistics_min_max_projection%' OR explain LIKE '%ReadFromMergeTree%';

SELECT 'statistics can also be materialized by the insert';
SET materialize_statistics_on_insert = 1;
INSERT INTO t_min_max_from_stats VALUES (2000000, 600, '2019-05-05', 43, 'bar');
SELECT min(date), max(date), min(value), max(value), count() FROM t_min_max_from_stats;
SELECT count() FROM (EXPLAIN SELECT min(date), max(date) FROM t_min_max_from_stats) WHERE explain LIKE '%_statistics_min_max_projection%' OR explain LIKE '%ReadFromMergeTree%';
SYSTEM START MERGES t_min_max_from_stats;
OPTIMIZE TABLE t_min_max_from_stats FINAL;

SELECT 'not applied: the setting is disabled';
SELECT count() FROM (EXPLAIN SELECT min(date), max(date) FROM t_min_max_from_stats SETTINGS use_statistics_for_min_max_aggregation = 0) WHERE explain LIKE '%_statistics_min_max_projection%';

SELECT 'not applied: Nullable column';
SELECT min(nullable_value), max(nullable_value) FROM t_min_max_from_stats;
SELECT count() FROM (EXPLAIN SELECT min(nullable_value), max(nullable_value) FROM t_min_max_from_stats) WHERE explain LIKE '%_statistics_min_max_projection%';

SELECT 'not applied: no statistics for String columns';
SELECT min(str), max(str) FROM t_min_max_from_stats;
SELECT count() FROM (EXPLAIN SELECT min(str), max(str) FROM t_min_max_from_stats) WHERE explain LIKE '%_statistics_min_max_projection%';

SELECT 'not applied: there is a filter';
SELECT min(date), max(date) FROM t_min_max_from_stats WHERE key < 1000;
SELECT count() FROM (EXPLAIN SELECT min(date), max(date) FROM t_min_max_from_stats WHERE key < 1000) WHERE explain LIKE '%_statistics_min_max_projection%';

SELECT 'applied: monotonic arithmetic is rewritten to be applied over the aggregation';
SELECT min(date + 1), max(date + 1) FROM t_min_max_from_stats;
SELECT count() FROM (EXPLAIN SELECT min(date + 1), max(date + 1) FROM t_min_max_from_stats) WHERE explain LIKE '%_statistics_min_max_projection%';

SELECT 'not applied: aggregation over an expression';
SELECT min(key % 7), max(key % 7) FROM t_min_max_from_stats;
SELECT count() FROM (EXPLAIN SELECT min(key % 7), max(key % 7) FROM t_min_max_from_stats) WHERE explain LIKE '%_statistics_min_max_projection%';

SELECT 'not applied: unsupported aggregate function';
SELECT min(date), sum(value) FROM t_min_max_from_stats;
SELECT count() FROM (EXPLAIN SELECT min(date), sum(value) FROM t_min_max_from_stats) WHERE explain LIKE '%_statistics_min_max_projection%';

SELECT 'not applied: lightweight delete';
DELETE FROM t_min_max_from_stats WHERE date = '2021-05-05';
SELECT min(date), max(date), count() FROM t_min_max_from_stats;
SELECT count() FROM (EXPLAIN SELECT min(date), max(date) FROM t_min_max_from_stats) WHERE explain LIKE '%_statistics_min_max_projection%';

SELECT 'the merge applies the delete and rebuilds statistics';
OPTIMIZE TABLE t_min_max_from_stats FINAL;
SELECT min(date), max(date), count() FROM t_min_max_from_stats;
SELECT count() FROM (EXPLAIN SELECT min(date), max(date) FROM t_min_max_from_stats) WHERE explain LIKE '%_statistics_min_max_projection%';

SELECT 'not applied: count over an expression must still evaluate the argument';
-- Disable the trivial count optimization: it intentionally answers a bare `count(expr)` over
-- a non-Nullable expression from metadata, which would mask what is being tested here.
SELECT count(key % 7) FROM t_min_max_from_stats SETTINGS optimize_trivial_count_query = 0;
SELECT count() FROM (EXPLAIN SELECT count(key % 7) FROM t_min_max_from_stats SETTINGS optimize_trivial_count_query = 0) WHERE explain LIKE '%_statistics_min_max_projection%';
-- Mixed with `min`, so no other shortcut applies: the statistics optimization must decline and
-- the expression must be evaluated (and throw).
SELECT min(value), count(throwIf(key = 0)) FROM t_min_max_from_stats; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

DROP TABLE t_min_max_from_stats;
