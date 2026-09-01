DROP TABLE IF EXISTS t_keycond_nan_norm;
-- Without the `basic` statistics there is no second pruner, so the granule counts below are
-- attributable to key analysis alone.
CREATE TABLE t_keycond_nan_norm (f Float64) ENGINE = MergeTree ORDER BY toString(f)
SETTINGS index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_keycond_nan_norm VALUES (nan), (2);

-- `f != 'nan'` is true for both rows: the constant is read as a Float64, where a NaN equals no value.
SELECT count() FROM t_keycond_nan_norm WHERE f != 'nan';
SELECT count() FROM t_keycond_nan_norm WHERE f != 'nan' SETTINGS use_primary_key = 0;

-- The same constant inside a tuple, where the NaN sits below the top level of the Field.
DROP TABLE IF EXISTS t_keycond_nan_norm_tuple;
CREATE TABLE t_keycond_nan_norm_tuple (k Tuple(Float64, Float64)) ENGINE = MergeTree
ORDER BY toString(k) SETTINGS index_granularity = 1;
INSERT INTO t_keycond_nan_norm_tuple VALUES ((nan, 1.)), ((3., 1.));

SELECT count() FROM t_keycond_nan_norm_tuple WHERE k != ('nan', 1.);
SELECT count() FROM t_keycond_nan_norm_tuple WHERE k != ('nan', 1.) SETTINGS use_primary_key = 0;

-- No row equals a NaN, and `count()` must come from the filter, not from the implicit minmax-count
-- projection.
SELECT count() FROM t_keycond_nan_norm WHERE f = 'nan'
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;
-- The atom is kept and relaxed rather than declined: declining leaves no key condition in the plan.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT * FROM t_keycond_nan_norm WHERE f = 'nan')
WHERE explain ILIKE '%Condition:%toString%';
-- A point lookup on an ordinary constant must still reach only its own granule.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keycond_nan_norm WHERE f = '2')
WHERE explain ILIKE '%Granules: 1/%';

DROP TABLE t_keycond_nan_norm_tuple;
DROP TABLE t_keycond_nan_norm;
