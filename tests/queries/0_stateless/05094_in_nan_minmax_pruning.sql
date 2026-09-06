-- Tags: no-parallel-replicas
-- https://github.com/ClickHouse/ClickHouse/issues/116927
-- `IN` matches `NaN` bit-exactly, unlike a comparison, but min/max-based pruning works with ranges
-- produced by `getExtremes`, which skips `NaN`. A part or granule holding `NaN` next to finite values
-- got a `NaN`-free range and was pruned even though the rows match.

SELECT nan IN (nan), nan = nan;

SELECT 'statistics part pruning';
DROP TABLE IF EXISTS t_nan_stats;
CREATE TABLE t_nan_stats (k UInt64, f Float64) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES t_nan_stats;
INSERT INTO t_nan_stats SELECT number, if(number < 13, nan, 1.5) FROM numbers(100000);
INSERT INTO t_nan_stats SELECT number + 200000, 2.5 FROM numbers(1000);

SELECT count() FROM t_nan_stats WHERE isNaN(f);
SELECT count() FROM t_nan_stats WHERE f IN (nan);
SELECT count() FROM t_nan_stats WHERE f IN (nan) SETTINGS use_statistics_for_part_pruning = 0;
SELECT count() FROM t_nan_stats WHERE f IN (nan, 2.5);
SELECT count() FROM t_nan_stats WHERE f IN (nan, 2.5) SETTINGS use_statistics_for_part_pruning = 0;
SELECT count() FROM t_nan_stats WHERE f IN (SELECT nan);
SELECT count() FROM t_nan_stats WHERE f IN (1.5);
SELECT count() FROM t_nan_stats WHERE f = nan;
DROP TABLE t_nan_stats;

SELECT 'minmax skip index';
DROP TABLE IF EXISTS t_nan_minmax;
CREATE TABLE t_nan_minmax (k UInt64, f Float64, INDEX mm f TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1000;
INSERT INTO t_nan_minmax SELECT number, if(number < 13, nan, 1.5) FROM numbers(100000);

SELECT count() FROM t_nan_minmax WHERE f IN (nan) SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_nan_minmax WHERE f IN (nan) SETTINGS use_skip_indexes = 1, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_nan_minmax WHERE f IN (SELECT nan) SETTINGS use_skip_indexes = 1, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_nan_minmax WHERE f IN (nan, 1.5) SETTINGS use_skip_indexes = 1, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_nan_minmax WHERE f IN (1.5) SETTINGS use_skip_indexes = 1, use_statistics_for_part_pruning = 0;
DROP TABLE t_nan_minmax;

SELECT 'a NaN-free set still prunes';
DROP TABLE IF EXISTS t_no_nan;
CREATE TABLE t_no_nan (k UInt64, f Float64, INDEX mm f TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1000;
INSERT INTO t_no_nan SELECT number, number FROM numbers(100000);
SELECT count() FROM t_no_nan WHERE f IN (5.0, 7.0) SETTINGS use_statistics_for_part_pruning = 0;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT k FROM t_no_nan WHERE f IN (5.0, 7.0) SETTINGS use_statistics_for_part_pruning = 0) WHERE explain LIKE '%Granules: 1/100%';
DROP TABLE t_no_nan;

SELECT 'an integer key is unaffected';
DROP TABLE IF EXISTS t_int_key;
CREATE TABLE t_int_key (k UInt64, INDEX mm k TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1000;
INSERT INTO t_int_key SELECT number FROM numbers(100000);
SELECT count() FROM t_int_key WHERE k IN (5, 7);
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT k FROM t_int_key WHERE k IN (5, 7)) WHERE explain LIKE '%Granules: 1/100%';
DROP TABLE t_int_key;
