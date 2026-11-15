-- Index analysis models `divide` and `multiply` by a constant as monotonic from the constant alone, but a
-- `Float` key column may hold `±inf`, and `-inf / inf`, `inf * 0` and `0 * inf` are all `NaN`. The
-- transformed range endpoint became `NaN`, which no point compares into, so every part and granule was
-- pruned and the query silently returned nothing - including the finite rows that do match.

DROP TABLE IF EXISTS t_monotonicity_nan;
CREATE TABLE t_monotonicity_nan (k Float64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_monotonicity_nan VALUES (-inf), (1), (2), (3);

SELECT 'a stored -inf endpoint mapped to NaN by the transform';
SELECT count() FROM t_monotonicity_nan WHERE k / inf = 0;
SELECT count() FROM t_monotonicity_nan WHERE k * 0 = 0;
SELECT count() FROM t_monotonicity_nan WHERE k / inf = 0 SETTINGS use_primary_key = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_monotonicity_nan WHERE k * 0 = 0 SETTINGS use_primary_key = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM values('k Float64', (-inf), (1), (2), (3)) WHERE k / inf = 0;

SELECT 'each pruning layer on its own';
SELECT count() FROM t_monotonicity_nan WHERE k / inf = 0 SETTINGS use_statistics_for_part_pruning = 0;
SELECT count() FROM t_monotonicity_nan WHERE k / inf = 0 SETTINGS use_primary_key = 0;

SELECT 'a constant that maps a stored 0 to NaN';
DROP TABLE IF EXISTS t_monotonicity_nan_zero;
CREATE TABLE t_monotonicity_nan_zero (k Float64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_monotonicity_nan_zero VALUES (0), (1), (2), (3);
SELECT count() FROM t_monotonicity_nan_zero WHERE k * inf = inf;
SELECT count() FROM t_monotonicity_nan_zero WHERE k * inf = inf SETTINGS use_primary_key = 0, use_statistics_for_part_pruning = 0;

SELECT 'pruning still applies where no endpoint becomes NaN';
DROP TABLE IF EXISTS t_monotonicity_finite;
CREATE TABLE t_monotonicity_finite (k Float64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_monotonicity_finite SELECT number FROM numbers(100);
SELECT count() FROM t_monotonicity_finite WHERE k / 2 > 40;
SELECT count() FROM t_monotonicity_finite WHERE k * 2 > 80;
SELECT count() FROM t_monotonicity_finite WHERE k / 2 > 40 SETTINGS use_primary_key = 0, use_statistics_for_part_pruning = 0;
-- Granule counts depend on settings the test runner randomizes, so compare the two numbers rather than
-- pinning them: the point is that some granules are still skipped.
SELECT toUInt64OrZero(extract(explain, 'Granules: (\\d+)/')) < toUInt64OrZero(extract(explain, 'Granules: \\d+/(\\d+)')) AS granules_pruned
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_monotonicity_finite WHERE k / 2 > 40)
WHERE explain LIKE '%Granules: %/%';

DROP TABLE t_monotonicity_finite;
DROP TABLE t_monotonicity_nan_zero;
DROP TABLE t_monotonicity_nan;
