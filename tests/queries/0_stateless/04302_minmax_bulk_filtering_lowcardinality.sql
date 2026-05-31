-- Tags: no-parallel-replicas
-- no-parallel-replicas: per-query SETTINGS toggling skip-index evaluation paths
-- must take effect on the executing replica.

-- Regression test: a minmax skip index over a `LowCardinality(Nullable(T))` column used to
-- trip an `assert_cast<ColumnUInt8>` in the bulk-filtering path (`Bad cast from type
-- DB::ColumnNullable`). The fast-path builder only skipped directly-`Nullable` index columns,
-- so a `LowCardinality(Nullable(T))` column slipped through; comparisons over it still produce
-- a `Nullable(UInt8)` mask. Such columns must fall back to the generic per-granule path, and
-- the bulk path must return the same answer as the generic path.

SET secondary_indices_enable_bulk_filtering = 1;
SET use_skip_indexes_on_data_read = 0;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_bulk_lcn;

CREATE TABLE t_bulk_lcn
(
    t LowCardinality(Nullable(DateTime)),
    v Int64,
    INDEX idx_t t TYPE minmax GRANULARITY 1,
    INDEX idx_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 128, allow_nullable_key = 1;

INSERT INTO t_bulk_lcn
SELECT
    if(number % 50 = 0, NULL, toDateTime('2024-01-01 00:00:00') + INTERVAL number SECOND) AS t,
    number AS v
FROM numbers(20000);

-- The crashing query shape: a `LowCardinality(Nullable(DateTime))` minmax column under
-- `use_minmax_index_bulk_filtering = 1`. It must not throw and must agree with the generic path.
SELECT 'bulk lowcardinality-nullable parity',
    length(groupUniqArray(c)) = 1 AS all_equal,
    any(c) AS count
FROM
(
    SELECT count() AS c FROM t_bulk_lcn
    WHERE t >= '2024-01-01 00:00:00' AND t < '2024-01-10 00:00:00' AND (v < 100 OR v > 19900)
    SETTINGS use_minmax_index_bulk_filtering = 0, use_skip_indexes_for_disjunctions = 0
    UNION ALL
    SELECT count() AS c FROM t_bulk_lcn
    WHERE t >= '2024-01-01 00:00:00' AND t < '2024-01-10 00:00:00' AND (v < 100 OR v > 19900)
    SETTINGS use_minmax_index_bulk_filtering = 1, use_skip_indexes_for_disjunctions = 0
    UNION ALL
    SELECT count() AS c FROM t_bulk_lcn
    WHERE t >= '2024-01-01 00:00:00' AND t < '2024-01-10 00:00:00' AND (v < 100 OR v > 19900)
    SETTINGS use_minmax_index_bulk_filtering = 1, use_skip_indexes_for_disjunctions = 1
);

DROP TABLE t_bulk_lcn;

-- A non-nullable `LowCardinality(Float)` minmax index does use the bulk fast path. The `NaN`
-- handling keyed on `WhichDataType(column_type).isFloat()` used to be skipped for it (the type is
-- `LowCardinality`, not directly `Float`), so a `[finite_min, NaN]` granule was wrongly pruned for
-- a lower-bounded predicate. Check parity: the granule containing `3.0` must be kept under bulk.
DROP TABLE IF EXISTS t_bulk_lc_nan;

CREATE TABLE t_bulk_lc_nan
(
    f LowCardinality(Float64),
    INDEX idx_f f TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO t_bulk_lc_nan VALUES (1.0), (2.0), (3.0), (nan);
INSERT INTO t_bulk_lc_nan VALUES (-10.0), (-9.0), (-8.0), (-7.0);

SELECT 'bulk lowcardinality-float nan parity',
    length(groupUniqArray(c)) = 1 AS all_equal,
    any(c) AS count
FROM
(
    SELECT count() AS c FROM t_bulk_lc_nan WHERE f >= 2.5
    SETTINGS use_minmax_index_bulk_filtering = 0
    UNION ALL
    SELECT count() AS c FROM t_bulk_lc_nan WHERE f >= 2.5
    SETTINGS use_minmax_index_bulk_filtering = 1
);

DROP TABLE t_bulk_lc_nan;
