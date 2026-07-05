-- Tags: no-random-settings, no-random-merge-tree-settings, no-debug

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_lazy_final;

CREATE TABLE t_lazy_final
(
    timestamp DateTime,
    id UInt64,
    version UInt64,
    status String,
    value Float64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (toStartOfDay(timestamp), id)
SETTINGS index_granularity = 256;

system stop merges t_lazy_final;

-- Insert data in multiple parts.
INSERT INTO t_lazy_final SELECT
    toDateTime('2024-01-01 00:00:00') + number * 60,
    number % 1000,
    1,
    if(number % 3 = 0, 'active', 'inactive'),
    number * 1.5
FROM numbers(5000);

INSERT INTO t_lazy_final SELECT
    toDateTime('2024-01-01 00:00:00') + number * 60,
    number % 1000,
    2,
    if(number % 3 = 0, 'deleted', 'active'),
    number * 2.0
FROM numbers(3000);

INSERT INTO t_lazy_final SELECT
    toDateTime('2024-01-05 00:00:00') + number * 60,
    number % 500,
    3,
    'active',
    number * 3.0
FROM numbers(2000);

-- Verify results match with and without optimization.

SELECT '-- filter on status';
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active'
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active'
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0;

SELECT '-- filter on timestamp range';
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE toStartOfDay(timestamp) = '2024-01-01'
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE toStartOfDay(timestamp) = '2024-01-01'
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0;

SELECT '-- combined filter';
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active' AND id < 100
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active' AND id < 100
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0;

-- Fallback: set row limit so small that the set will be truncated. Results must still be correct.
SELECT '-- small row limit (fallback)';
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active'
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10;

-- Fallback: set byte limit very small.
SELECT '-- small byte limit (fallback)';
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active'
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0, max_bytes_for_lazy_final = 100;

-- PREWHERE.
SELECT '-- prewhere';
SELECT count(), sum(value) FROM t_lazy_final FINAL PREWHERE id < 200 WHERE status = 'active'
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT count(), sum(value) FROM t_lazy_final FINAL PREWHERE id < 200 WHERE status = 'active'
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0;

-- Row policy.
DROP ROW POLICY IF EXISTS policy_lazy_final ON t_lazy_final;
CREATE ROW POLICY policy_lazy_final ON t_lazy_final FOR SELECT USING id < 500 TO ALL;

SELECT '-- row policy';
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active'
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active'
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0;

DROP ROW POLICY policy_lazy_final ON t_lazy_final;

-- Plan checks: optimization enabled with filter should have InputSelector.
SELECT '-- plan: optimization has InputSelector';
SELECT explain LIKE '%InputSelector%' FROM (
    EXPLAIN actions = 0
    SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0
) WHERE explain LIKE '%InputSelector%';

-- Plan checks: optimization disabled should not have InputSelector.
SELECT '-- plan: no optimization has no InputSelector';
SELECT count() FROM (
    EXPLAIN actions = 0
    SELECT count(), sum(value) FROM t_lazy_final FINAL WHERE status = 'active'
    SETTINGS query_plan_optimize_lazy_final = 0
) WHERE explain LIKE '%InputSelector%';

-- Plan checks: no filter means optimization should not apply.
SELECT '-- plan: no filter has no InputSelector';
SELECT count() FROM (
    EXPLAIN actions = 0
    SELECT count(), sum(value) FROM t_lazy_final FINAL
    SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0
) WHERE explain LIKE '%InputSelector%';

DROP TABLE t_lazy_final;

-- Test tiebreaker: when versions are equal, last inserted row should win.
DROP TABLE IF EXISTS t_lazy_final_tiebreak;
CREATE TABLE t_lazy_final_tiebreak (key UInt64, version UInt64, value String)
ENGINE = ReplacingMergeTree(version) ORDER BY key SETTINGS index_granularity = 256;

SYSTEM STOP MERGES t_lazy_final_tiebreak;
INSERT INTO t_lazy_final_tiebreak VALUES (1, 1, 'first');
INSERT INTO t_lazy_final_tiebreak VALUES (1, 1, 'second');
INSERT INTO t_lazy_final_tiebreak VALUES (1, 1, 'third');

SELECT '-- tiebreaker: same version';
SELECT value FROM t_lazy_final_tiebreak FINAL WHERE value != ''
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT value FROM t_lazy_final_tiebreak FINAL WHERE value != ''
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0;

DROP TABLE t_lazy_final_tiebreak;

-- Test with signed version column.
DROP TABLE IF EXISTS t_lazy_final_signed;
CREATE TABLE t_lazy_final_signed (key UInt64, version Int64, value String)
ENGINE = ReplacingMergeTree(version) ORDER BY key SETTINGS index_granularity = 256;

SYSTEM STOP MERGES t_lazy_final_signed;
INSERT INTO t_lazy_final_signed VALUES (1, -1, 'negative');
INSERT INTO t_lazy_final_signed VALUES (1, 5, 'positive');
INSERT INTO t_lazy_final_signed VALUES (1, 5, 'positive_last');

SELECT '-- signed version';
SELECT value FROM t_lazy_final_signed FINAL WHERE value != ''
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT value FROM t_lazy_final_signed FINAL WHERE value != ''
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0;

DROP TABLE t_lazy_final_signed;

-- Test with Int32 version column.
DROP TABLE IF EXISTS t_lazy_final_int32;
CREATE TABLE t_lazy_final_int32 (key UInt64, version Int32, value String)
ENGINE = ReplacingMergeTree(version) ORDER BY key SETTINGS index_granularity = 256;

SYSTEM STOP MERGES t_lazy_final_int32;
INSERT INTO t_lazy_final_int32 VALUES (1, -100, 'negative');
INSERT INTO t_lazy_final_int32 VALUES (1, 10, 'positive');
INSERT INTO t_lazy_final_int32 VALUES (1, 10, 'positive_last');

SELECT '-- Int32 version';
SELECT value FROM t_lazy_final_int32 FINAL WHERE value != ''
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT value FROM t_lazy_final_int32 FINAL WHERE value != ''
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0;

DROP TABLE t_lazy_final_int32;

-- Test with UInt128 version (tuple branch).
DROP TABLE IF EXISTS t_lazy_final_uint128;
CREATE TABLE t_lazy_final_uint128 (key UInt64, version UInt128, value String)
ENGINE = ReplacingMergeTree(version) ORDER BY key SETTINGS index_granularity = 256;

SYSTEM STOP MERGES t_lazy_final_uint128;
INSERT INTO t_lazy_final_uint128 VALUES (1, 1, 'first');
INSERT INTO t_lazy_final_uint128 VALUES (1, 100, 'second');
INSERT INTO t_lazy_final_uint128 VALUES (1, 100, 'second_last');

SELECT '-- UInt128 version';
SELECT value FROM t_lazy_final_uint128 FINAL WHERE value != ''
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT value FROM t_lazy_final_uint128 FINAL WHERE value != ''
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0;

DROP TABLE t_lazy_final_uint128;

-- Test with Int128 version (tuple branch), including negative version.
DROP TABLE IF EXISTS t_lazy_final_int128;
CREATE TABLE t_lazy_final_int128 (key UInt64, version Int128, value String)
ENGINE = ReplacingMergeTree(version) ORDER BY key SETTINGS index_granularity = 256;

SYSTEM STOP MERGES t_lazy_final_int128;
INSERT INTO t_lazy_final_int128 VALUES (1, -1000, 'negative');
INSERT INTO t_lazy_final_int128 VALUES (1, 50, 'positive');
INSERT INTO t_lazy_final_int128 VALUES (1, 50, 'positive_last');

SELECT '-- Int128 version';
SELECT value FROM t_lazy_final_int128 FINAL WHERE value != ''
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT value FROM t_lazy_final_int128 FINAL WHERE value != ''
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0;

DROP TABLE t_lazy_final_int128;

-- Test with Date32 version column (can have negative values for pre-epoch dates).
DROP TABLE IF EXISTS t_lazy_final_date32;
CREATE TABLE t_lazy_final_date32 (key UInt64, version Date32, value String)
ENGINE = ReplacingMergeTree(version) ORDER BY key SETTINGS index_granularity = 256;

SYSTEM STOP MERGES t_lazy_final_date32;
INSERT INTO t_lazy_final_date32 VALUES (1, '1900-01-01', 'pre_epoch');
INSERT INTO t_lazy_final_date32 VALUES (1, '2024-01-01', 'post_epoch');
INSERT INTO t_lazy_final_date32 VALUES (1, '2024-01-01', 'post_epoch_last');

SELECT '-- Date32 version';
SELECT value FROM t_lazy_final_date32 FINAL WHERE value != ''
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT value FROM t_lazy_final_date32 FINAL WHERE value != ''
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0;

DROP TABLE t_lazy_final_date32;

-- Test with DateTime64 version column (tuple fallback path, includes pre-epoch negative values).
DROP TABLE IF EXISTS t_lazy_final_dt64;
CREATE TABLE t_lazy_final_dt64 (key UInt64, version DateTime64(3, 'UTC'), value String)
ENGINE = ReplacingMergeTree(version) ORDER BY key SETTINGS index_granularity = 256;

SYSTEM STOP MERGES t_lazy_final_dt64;
INSERT INTO t_lazy_final_dt64 VALUES (1, '1900-01-01 00:00:00.000', 'pre_epoch');
INSERT INTO t_lazy_final_dt64 VALUES (1, '2024-06-15 12:30:00.500', 'post_epoch');
INSERT INTO t_lazy_final_dt64 VALUES (1, '2024-06-15 12:30:00.500', 'post_epoch_last');

SELECT '-- DateTime64 version';
SELECT value FROM t_lazy_final_dt64 FINAL WHERE value != ''
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT value FROM t_lazy_final_dt64 FINAL WHERE value != ''
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0;

DROP TABLE t_lazy_final_dt64;

-- Test with no version column.
DROP TABLE IF EXISTS t_lazy_final_noversion;
CREATE TABLE t_lazy_final_noversion (key UInt64, value String)
ENGINE = ReplacingMergeTree() ORDER BY key SETTINGS index_granularity = 256;

SYSTEM STOP MERGES t_lazy_final_noversion;
INSERT INTO t_lazy_final_noversion VALUES (1, 'first');
INSERT INTO t_lazy_final_noversion VALUES (1, 'second');

SELECT '-- no version column';
SELECT value FROM t_lazy_final_noversion FINAL WHERE value != ''
SETTINGS query_plan_optimize_lazy_final = 0;
SELECT value FROM t_lazy_final_noversion FINAL WHERE value != ''
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0;

DROP TABLE t_lazy_final_noversion;
