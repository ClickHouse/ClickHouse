-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.

-- materialize_statistics_on_insert is orthogonal to minmax NaN pruning and, when randomized on, makes
-- even the use_skip_indexes=0 reference count wrong for the NaN rows, so pin it off.
SET materialize_statistics_on_insert = 0;

-- getExtremes skips NaN, so a granule (1.0, nan, 3.0) stores the finite bound [1, 3] that hides the NaN.
-- `NOT ((val >= a) AND (val <= b))` is satisfied by a NaN row, because `NaN >= a` is false, so a float
-- bound must never be reported as fully satisfying a condition. Every arm asserts the skip-index result
-- against the same query without the skip index.

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

-- The pair above reads the same value when the index kept the granule and when the index was dropped from
-- the plan altogether, so assert this layer is still there: an index whose condition is unusable never
-- enters useful_indices and its whole Skip block, this token included, disappears.
SELECT countIf(explain LIKE '%Description: minmax GRANULARITY 1%')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_minmax_nan_mixed WHERE NOT ((val >= 0.) AND (val <= 3.)));

-- A positive range is decided by intersection alone, and a hidden NaN satisfies no comparison, so it
-- can never make one true: both granules are pruned for val > 500.
SELECT count() FROM t_minmax_nan_mixed WHERE val > 500;
SELECT count() FROM t_minmax_nan_mixed WHERE val > 500 SETTINGS use_skip_indexes = 0;
SELECT countIf(explain LIKE '%Granules: 0/2%') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_minmax_nan_mixed WHERE val > 500);

-- 1.0 sits in the NaN granule, so positive equality on it must still find the row.
SELECT count() FROM t_minmax_nan_mixed WHERE val = 1.0;
SELECT count() FROM t_minmax_nan_mixed WHERE val = 1.0 SETTINGS use_skip_indexes = 0;

-- The stored bound is a semantic extremum, so a consumer that reads it as one gets the true maximum.
-- `ORDER BY val DESC LIMIT 1` reaches two independently gated TopK mechanisms: plan-time granule
-- ordering and the read-time threshold tracker. Both are admitted if either gate is on, so the oracle
-- has to turn off both, and pin both on for the other arm because both gates are randomized.
SELECT val FROM t_minmax_nan_mixed ORDER BY val DESC LIMIT 1
SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1;
SELECT val FROM t_minmax_nan_mixed ORDER BY val DESC LIMIT 1
SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 0;

-- A monotonic function chain is applied to the bound's endpoints, never to the hidden NaN.
SELECT count() FROM t_minmax_nan_mixed WHERE NOT (val * 2 > 6);
SELECT count() FROM t_minmax_nan_mixed WHERE NOT (val * 2 > 6) SETTINGS use_skip_indexes = 0;

DROP TABLE t_minmax_nan_mixed;

DROP TABLE IF EXISTS t_minmax_nan_chain;

CREATE TABLE t_minmax_nan_chain
(id UInt64, val Float64, INDEX idx_val val TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_minmax_nan_chain VALUES (1, -3.0), (2, nan), (3, -1.0);
INSERT INTO t_minmax_nan_chain VALUES (4, 100.0), (5, 150.0), (6, 200.0);

-- A monotonicity-declared function need not preserve NaN: sign(nan) = 1, so the NaN row matches
-- while the transformed bound [-1, -1] does not, and the result type stays Float64.
SELECT count() FROM t_minmax_nan_chain WHERE toFloat64(sign(val)) > 0;
SELECT count() FROM t_minmax_nan_chain WHERE toFloat64(sign(val)) > 0 SETTINGS use_skip_indexes = 0;

-- A set atom carries its own chain per key mapping, so the chain rule has to be applied there too.
SELECT count() FROM t_minmax_nan_chain WHERE sign(val) IN (1);
SELECT count() FROM t_minmax_nan_chain WHERE sign(val) IN (1) SETTINGS use_skip_indexes = 0;

DROP TABLE t_minmax_nan_chain;

-- Set atoms reach "the whole range satisfies this" through a different branch than range atoms, and a
-- single-point bound is what takes it: a granule whose only finite value is 1.0 stores [1, 1].
DROP TABLE IF EXISTS t_minmax_nan_set;

CREATE TABLE t_minmax_nan_set
(id UInt64, val Float64, INDEX idx_val val TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_minmax_nan_set VALUES (1, 1.0), (2, nan), (3, nan);
INSERT INTO t_minmax_nan_set VALUES (4, 2.0), (5, 2.0), (6, 2.0);

SELECT count() FROM t_minmax_nan_set WHERE val NOT IN (1.);
SELECT count() FROM t_minmax_nan_set WHERE val NOT IN (1.) SETTINGS use_skip_indexes = 0;

-- A set matches its elements under the total order, in which nan = nan, so a NaN element can be
-- matched without any negation: `val IN (nan)` must return the NaN rows.
SELECT count() FROM t_minmax_nan_set WHERE val IN (nan);
SELECT count() FROM t_minmax_nan_set WHERE val IN (nan) SETTINGS use_skip_indexes = 0;

-- An ordinary IN list holds no NaN, so it keeps pruning: only the [2, 2] granule is read.
SELECT countIf(explain LIKE '%Granules: 1/2%') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_minmax_nan_set WHERE val IN (2., 5.));

DROP TABLE t_minmax_nan_set;

-- A set skip index stores its own hyperrectangle, also built with getExtremes, and uses it as a
-- pre-check before the exact per-value evaluation. The bulk and per-granule entry points differ.
DROP TABLE IF EXISTS t_set_idx_nan;

CREATE TABLE t_set_idx_nan
(id UInt64, val Float64, INDEX idx_val val TYPE set(0) GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_set_idx_nan VALUES (1, 1.0), (2, nan), (3, 3.0);
INSERT INTO t_set_idx_nan VALUES (4, 100.0), (5, 150.0), (6, 200.0);

SELECT count() FROM t_set_idx_nan WHERE NOT ((val >= 0.) AND (val <= 3.));
SELECT count() FROM t_set_idx_nan WHERE NOT ((val >= 0.) AND (val <= 3.)) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_set_idx_nan WHERE NOT ((val >= 0.) AND (val <= 3.)) SETTINGS secondary_indices_enable_bulk_filtering = 0;
SELECT count() FROM t_set_idx_nan WHERE NOT ((val >= 0.) AND (val <= 3.)) SETTINGS secondary_indices_enable_bulk_filtering = 1;

-- Same liveness assertion for this layer, on its own negated predicate and its own index type.
SELECT countIf(explain LIKE '%Description: set GRANULARITY 1%')
FROM (EXPLAIN indexes = 1 SELECT count() FROM t_set_idx_nan WHERE NOT ((val >= 0.) AND (val <= 3.)));

DROP TABLE t_set_idx_nan;

-- Tuple comparison is built from its elements' scalar comparisons, so a NaN element makes an ordering
-- operator and its inverse both false exactly as a bare float does, and ColumnTuple::getExtremes
-- delegates per child.
DROP TABLE IF EXISTS t_minmax_nan_tuple;

CREATE TABLE t_minmax_nan_tuple
(id UInt64, t Tuple(Float64, Float64), INDEX idx_t t TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_minmax_nan_tuple VALUES (1, (1., 1.)), (2, (nan, 1.)), (3, (3., 3.));
INSERT INTO t_minmax_nan_tuple VALUES (4, (100., 100.)), (5, (150., 150.)), (6, (200., 200.));

SELECT count() FROM t_minmax_nan_tuple WHERE NOT (t <= (100., 100.));
SELECT count() FROM t_minmax_nan_tuple WHERE NOT (t <= (100., 100.)) SETTINGS use_skip_indexes = 0;

-- A tuple constant holding a NaN: the column operand's own type is already NaN-hiding, so it is what
-- triggers the guard here.
SELECT count() FROM t_minmax_nan_tuple WHERE NOT (t < (nan, 1.));
SELECT count() FROM t_minmax_nan_tuple WHERE NOT (t < (nan, 1.)) SETTINGS use_skip_indexes = 0;

DROP TABLE t_minmax_nan_tuple;

-- A packed Tuple key keeps the mapped set column as a ColumnTuple, so a set element holding a NaN is
-- a tuple rather than a top-level NaN. A set matches under the total order, in which nan = nan, so
-- such an element can make a positive IN true and its granule must be kept. The finite element has to
-- fall outside the granule's bound, or it keeps the granule on its own and the arm asserts nothing.
DROP TABLE IF EXISTS t_minmax_nan_tuple_set;

CREATE TABLE t_minmax_nan_tuple_set
(id UInt64, t Tuple(Float64, Float64), INDEX idx_t t TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_minmax_nan_tuple_set VALUES (1, (1., 1.)), (2, (nan, 1.)), (3, (3., 3.));

SELECT count() FROM t_minmax_nan_tuple_set WHERE t IN ((nan, 1.), (500., 500.));
SELECT count() FROM t_minmax_nan_tuple_set WHERE t IN ((nan, 1.), (500., 500.)) SETTINGS use_skip_indexes = 0;

-- An all-finite set outside the bound is still pruned, so the rule did not disable this index.
SELECT count() FROM t_minmax_nan_tuple_set WHERE t IN ((500., 500.));
SELECT count() FROM t_minmax_nan_tuple_set WHERE t IN ((500., 500.)) SETTINGS use_skip_indexes = 0;

DROP TABLE t_minmax_nan_tuple_set;

-- Array and Map are excluded from the rule, so an all-finite Array minmax index keeps its pruning:
-- only the sibling granule is read for a negated range. An Array bound hides a NaN as much as a float
-- one does, but a NaN element is ordered, so it also makes a positive predicate true and would need a
-- stronger treatment than this rule applies. That carrier is out of scope, so no arm asserts its
-- result and this fixture holds no NaN.
DROP TABLE IF EXISTS t_minmax_nan_array;

CREATE TABLE t_minmax_nan_array
(id UInt64, a Array(Float64), INDEX idx_a a TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_minmax_nan_array VALUES (1, [1.]), (2, [2.]), (3, [3.]);
INSERT INTO t_minmax_nan_array VALUES (4, [100.]), (5, [150.]), (6, [200.]);

SELECT count() FROM t_minmax_nan_array WHERE NOT (a <= [3.]);
SELECT count() FROM t_minmax_nan_array WHERE NOT (a <= [3.]) SETTINGS use_skip_indexes = 0;
SELECT countIf(explain LIKE '%Granules: 1/2%') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_minmax_nan_array WHERE NOT (a <= [3.]));

DROP TABLE t_minmax_nan_array;

-- LowCardinality(Float*) and LowCardinality(Nullable(Float*)) hide a NaN the same way, so the rule has
-- to see through both wrappers.

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
-- that the granule contains NULL, so that bound is already non-extremal for a different reason. Both
-- `IS NULL` and the negated comparison must keep the granule.

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

-- The NULL+NaN granule's stored range is [1, +Inf] because of the NULLS_LAST sentinel, so val > 500
-- intersects it and it is read; the sibling all-finite granule [100, 200] is still pruned (1/2).
SELECT count() FROM t_minmax_null_nan WHERE val > 500;
SELECT count() FROM t_minmax_null_nan WHERE val > 500 SETTINGS use_skip_indexes = 0;
SELECT countIf(explain LIKE '%Granules: 1/2%') FROM (EXPLAIN indexes = 1 SELECT count() FROM t_minmax_null_nan WHERE val > 500);

DROP TABLE t_minmax_null_nan;

-- Single-sided negated comparison (issue #110266): NOT(f > c) is reduced by KeyCondition to the range
-- f <= c, and a min=max=NaN granule reports "cannot match" and is pruned. But NOT(NaN > c) = NOT(false)
-- = true, so the NaN row must be returned. Same for NOT(f < c). Non-negated forms are unaffected.

DROP TABLE IF EXISTS t_minmax_nan_single;

CREATE TABLE t_minmax_nan_single
(f Float64, INDEX idx_f f TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 3, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_minmax_nan_single VALUES (nan);

-- The all-NaN granule satisfies both negations and must be returned (1 row each).
SELECT count() FROM t_minmax_nan_single WHERE NOT (f > 256);
SELECT count() FROM t_minmax_nan_single WHERE NOT (f > 256) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_minmax_nan_single WHERE NOT (f < 256);
SELECT count() FROM t_minmax_nan_single WHERE NOT (f < 256) SETTINGS use_skip_indexes = 0;

DROP TABLE t_minmax_nan_single;
