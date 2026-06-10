-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.

-- minmax skip index must not prune a granule that may contain NaN under a negated comparison range.
-- `NOT ((val >= a) AND (val <= b))` is satisfied by NaN rows (NaN >= a is false, so the negation is true),
-- but range analysis over the stored [min, max] hyperrectangle dropped such granules (issue #106948).
-- Two cases: an all-NaN granule and a granule mixing finite floats with NaN. The skip-index result must
-- match the result without the skip index in every case.

DROP TABLE IF EXISTS t_minmax_nan;

CREATE TABLE t_minmax_nan
(id UInt64, val Nullable(Float64), INDEX idx_val val TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_minmax_nan VALUES (1, NULL), (2, NULL), (3, NULL);
INSERT INTO t_minmax_nan VALUES (4, nan), (5, nan), (6, nan);
INSERT INTO t_minmax_nan VALUES (7, 1.0), (8, 2.0), (9, 3.0);

-- Exact issue reproducer: the all-NaN granule (rows 4,5,6) satisfies the negation and must be returned.
SELECT count() FROM t_minmax_nan WHERE NOT ((val >= 0.) AND (val <= 3.));
SELECT count() FROM t_minmax_nan WHERE NOT ((val >= 0.) AND (val <= 3.)) SETTINGS use_skip_indexes = 0;

DROP TABLE t_minmax_nan;

-- Mixed granule: NaN shares a granule with finite values, so the stored [min, max] looks finite ([1, 3])
-- and hides the NaN. The granule must still be kept for the negated range.

DROP TABLE IF EXISTS t_minmax_nan_mixed;

CREATE TABLE t_minmax_nan_mixed
(id UInt64, val Float64, INDEX idx_val val TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_minmax_nan_mixed VALUES (1, 1.0), (2, nan), (3, 3.0);
INSERT INTO t_minmax_nan_mixed VALUES (4, 100.0), (5, 150.0), (6, 200.0);

-- Row 2 (NaN) satisfies the negation; rows 4,5,6 also satisfy it. Expected 4 rows.
SELECT count() FROM t_minmax_nan_mixed WHERE NOT ((val >= 0.) AND (val <= 3.));
SELECT count() FROM t_minmax_nan_mixed WHERE NOT ((val >= 0.) AND (val <= 3.)) SETTINGS use_skip_indexes = 0;

-- A NaN-containing granule must still be pruned by a positive range it cannot satisfy: no value > 500.
SELECT count() FROM t_minmax_nan_mixed WHERE val > 500;
SELECT count() FROM t_minmax_nan_mixed WHERE val > 500 SETTINGS use_skip_indexes = 0;

-- Positive equality on a finite value sharing the NaN granule must still find it.
SELECT count() FROM t_minmax_nan_mixed WHERE val = 100.0;
SELECT count() FROM t_minmax_nan_mixed WHERE val = 100.0 SETTINGS use_skip_indexes = 0;

DROP TABLE t_minmax_nan_mixed;

-- LowCardinality(Float*): the minmax aggregator must materialize LowCardinality before checking for
-- NaN, otherwise the nested float column is invisible and a mixed LC granule (e.g. [1, nan, 3]) keeps a
-- clean stored range and is wrongly pruned for the negated comparison.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_minmax_nan_lc;

CREATE TABLE t_minmax_nan_lc
(id UInt64, val LowCardinality(Float64), INDEX idx_val val TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_minmax_nan_lc VALUES (1, 1.0), (2, nan), (3, 3.0);
INSERT INTO t_minmax_nan_lc VALUES (4, 100.0), (5, 150.0), (6, 200.0);

SELECT count() FROM t_minmax_nan_lc WHERE NOT ((val >= 0.) AND (val <= 3.));
SELECT count() FROM t_minmax_nan_lc WHERE NOT ((val >= 0.) AND (val <= 3.)) SETTINGS use_skip_indexes = 0;

DROP TABLE t_minmax_nan_lc;

DROP TABLE IF EXISTS t_minmax_nan_lcn;

CREATE TABLE t_minmax_nan_lcn
(id UInt64, val LowCardinality(Nullable(Float64)), INDEX idx_val val TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_minmax_nan_lcn VALUES (1, 1.0), (2, nan), (3, 3.0);
INSERT INTO t_minmax_nan_lcn VALUES (4, 100.0), (5, 150.0), (6, 200.0);

SELECT count() FROM t_minmax_nan_lcn WHERE NOT ((val >= 0.) AND (val <= 3.));
SELECT count() FROM t_minmax_nan_lcn WHERE NOT ((val >= 0.) AND (val <= 3.)) SETTINGS use_skip_indexes = 0;

DROP TABLE t_minmax_nan_lcn;

-- NULL and NaN in the same granule: getExtremesNullLast records the NULLS_LAST +inf sentinel to mark
-- that the granule contains NULL. The NaN-widening must not overwrite that sentinel, otherwise the
-- granule stops looking like it contains NULL and `val IS NULL` is wrongly pruned. Both `IS NULL` and
-- the negated comparison (which the NaN row satisfies) must keep the granule.

DROP TABLE IF EXISTS t_minmax_null_nan;

CREATE TABLE t_minmax_null_nan
(id UInt64, val Nullable(Float64), INDEX idx_val val TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_minmax_null_nan VALUES (1, NULL), (2, nan), (3, 1.0);
INSERT INTO t_minmax_null_nan VALUES (4, 100.0), (5, 150.0), (6, 200.0);

-- The granule with NULL+NaN+1.0 must be kept for IS NULL (1 row).
SELECT count() FROM t_minmax_null_nan WHERE val IS NULL;
SELECT count() FROM t_minmax_null_nan WHERE val IS NULL SETTINGS use_skip_indexes = 0;

-- The same granule's NaN row satisfies the negation; rows 4,5,6 also do. Expected 4 rows.
SELECT count() FROM t_minmax_null_nan WHERE NOT ((val >= 0.) AND (val <= 3.));
SELECT count() FROM t_minmax_null_nan WHERE NOT ((val >= 0.) AND (val <= 3.)) SETTINGS use_skip_indexes = 0;

-- A positive range no value satisfies must still prune the granule.
SELECT count() FROM t_minmax_null_nan WHERE val > 500;
SELECT count() FROM t_minmax_null_nan WHERE val > 500 SETTINGS use_skip_indexes = 0;

DROP TABLE t_minmax_null_nan;
