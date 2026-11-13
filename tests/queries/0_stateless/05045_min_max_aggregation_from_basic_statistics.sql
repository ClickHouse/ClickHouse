-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test checks EXPLAIN of plans with reading steps replaced by prepared sources.

-- The default `auto_statistics_types` is `basic, uniq_v2`, and unlike the deprecated `minmax` type,
-- `basic` is declared for every column type but records an exact min/max only for the numeric-like
-- ones. Check that the min/max aggregation shortcut answers from `basic` statistics exactly, and
-- that it declines the columns whose min/max `basic` does not track.

DROP TABLE IF EXISTS t_default_stats;
DROP TABLE IF EXISTS t_basic_stats;

-- The default value of `auto_statistics_types` is spelled out instead of being left out, because the
-- test runner randomizes every MergeTree setting a test does not set itself.
CREATE TABLE t_default_stats (key UInt64, value Int32, date Date, event_time DateTime, amount Decimal64(3), str String)
ENGINE = MergeTree ORDER BY key
SETTINGS auto_statistics_types = 'basic, uniq_v2', materialize_statistics_on_merge = 1;

CREATE TABLE t_basic_stats (key UInt64, value Int32, date Date, event_time DateTime, amount Decimal64(3), str String)
ENGINE = MergeTree ORDER BY key
SETTINGS auto_statistics_types = 'basic', materialize_statistics_on_merge = 1;

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET use_statistics_for_min_max_aggregation = 1;
SET materialize_statistics_on_insert = 1;
-- Pin every setting the optimization's eligibility depends on: the test runner randomizes
-- optimize_aggregation_in_order, and aggregation-in-order (like aggregate_functions_null_for_empty)
-- disables the projection optimization entirely, turning the EXPLAIN checks below into 0.
SET enable_analyzer = 1;
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0;
SET aggregate_functions_null_for_empty = 0;

INSERT INTO t_default_stats
    SELECT number, toInt32(number % 1000) - 500, toDate('2020-01-01') + number % 365,
        toDateTime('2020-01-01 00:00:00') + number, toDecimal64(number, 3) / 7, toString(number)
    FROM numbers(100000);

INSERT INTO t_basic_stats SELECT * FROM t_default_stats;

OPTIMIZE TABLE t_default_stats FINAL;
OPTIMIZE TABLE t_basic_stats FINAL;

SELECT 'the default auto_statistics_types: answered from statistics';
SELECT min(value), max(value), min(date), max(date), min(event_time), max(event_time), min(amount), max(amount), count() FROM t_default_stats;
SELECT 'the same values without the optimization';
SELECT min(value), max(value), min(date), max(date), min(event_time), max(event_time), min(amount), max(amount), count() FROM t_default_stats SETTINGS use_statistics_for_min_max_aggregation = 0;
SELECT count() FROM (EXPLAIN SELECT min(value), max(value), min(date), max(date), min(event_time), max(event_time), min(amount), max(amount), count() FROM t_default_stats) WHERE explain LIKE '%_statistics_min_max_projection%';

SELECT 'explicit auto_statistics_types = basic: answered from statistics';
SELECT min(value), max(value), min(date), max(date), min(event_time), max(event_time), min(amount), max(amount), count() FROM t_basic_stats;
SELECT 'the same values without the optimization';
SELECT min(value), max(value), min(date), max(date), min(event_time), max(event_time), min(amount), max(amount), count() FROM t_basic_stats SETTINGS use_statistics_for_min_max_aggregation = 0;
SELECT count() FROM (EXPLAIN SELECT min(value), max(value), min(date), max(date), min(event_time), max(event_time), min(amount), max(amount), count() FROM t_basic_stats) WHERE explain LIKE '%_statistics_min_max_projection%';

SELECT 'nothing is read when every part is answered from statistics';
SELECT count() FROM (EXPLAIN SELECT min(value), max(value), count() FROM t_default_stats) WHERE explain LIKE '%ReadFromMergeTree%';

SELECT 'not applied: basic statistics do not track the min/max of a String column';
SELECT min(str), max(str) FROM t_default_stats;
SELECT count() FROM (EXPLAIN SELECT min(str), max(str) FROM t_default_stats) WHERE explain LIKE '%_statistics_min_max_projection%';
SELECT count() FROM (EXPLAIN SELECT min(str), max(str) FROM t_basic_stats) WHERE explain LIKE '%_statistics_min_max_projection%';

SELECT 'not applied: a single unsupported column disables the whole aggregation';
SELECT min(value), max(str) FROM t_default_stats;
SELECT count() FROM (EXPLAIN SELECT min(value), max(str) FROM t_default_stats) WHERE explain LIKE '%_statistics_min_max_projection%';

DROP TABLE t_default_stats;
DROP TABLE t_basic_stats;
