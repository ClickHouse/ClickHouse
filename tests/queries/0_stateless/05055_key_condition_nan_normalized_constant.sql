DROP TABLE IF EXISTS t_keycond_nan_norm;
-- Without the `basic` statistics there is no second pruner, so the granule counts below are
-- attributable to key analysis alone.
CREATE TABLE t_keycond_nan_norm (f Float64) ENGINE = MergeTree ORDER BY toString(f)
SETTINGS index_granularity = 1, auto_statistics_types = '';
INSERT INTO t_keycond_nan_norm VALUES (nan), (2);

-- `f != 'nan'` is true for both rows: the comparison reads the constant as a Float64, where a NaN
-- equals no value. The index compared `toString(f)` with `'nan'` and dropped the granule holding the
-- NaN row.
SELECT count() FROM t_keycond_nan_norm WHERE f != 'nan';
SELECT count() FROM t_keycond_nan_norm WHERE f != 'nan' SETTINGS use_primary_key = 0;

-- The same constant inside a tuple, where the NaN sits below the top level of the Field.
DROP TABLE IF EXISTS t_keycond_nan_norm_tuple;
CREATE TABLE t_keycond_nan_norm_tuple (k Tuple(Float64, Float64)) ENGINE = MergeTree
ORDER BY toString(k) SETTINGS index_granularity = 1;
INSERT INTO t_keycond_nan_norm_tuple VALUES ((nan, 1.)), ((3., 1.));

SELECT count() FROM t_keycond_nan_norm_tuple WHERE k != ('nan', 1.);
SELECT count() FROM t_keycond_nan_norm_tuple WHERE k != ('nan', 1.) SETTINGS use_primary_key = 0;

-- No row equals a NaN. An exact range let `count()` be answered from the implicit minmax-count
-- projection instead of from the filter, which reported the granule as a match.
SELECT count() FROM t_keycond_nan_norm WHERE f = 'nan';
-- A point lookup on an ordinary constant still reaches only its own granule, so treating the NaN
-- constant as inexact did not cost the pruning that declining the atom would have.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keycond_nan_norm WHERE f = '2')
WHERE explain ILIKE '%Granules: 1/%';

DROP TABLE t_keycond_nan_norm_tuple;
DROP TABLE t_keycond_nan_norm;
