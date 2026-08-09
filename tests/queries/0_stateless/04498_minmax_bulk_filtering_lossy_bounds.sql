-- Tags: no-parallel-replicas
-- no-parallel-replicas: per-query SETTINGS toggling skip-index evaluation paths
-- must take effect on the executing replica.

-- Regression test for the minmax bulk-filtering path (use_minmax_index_bulk_filtering).
-- The bulk ActionsDAG path materializes each range bound as a constant column in the index
-- column type, so it compares everything in that single type. The scalar
-- KeyCondition::checkInHyperrectangle instead compares the original bound Field against the
-- granule min/max accurately *across types*. When the bound conversion is lossy, the bulk path
-- used to become strictly more selective than the scalar path and prune granules that actually
-- match, returning wrong results. These cases compare counts under use_minmax_index_bulk_filtering
-- = 0 and = 1; they must agree (and stay non-zero, i.e. the matching granule is kept).

SET secondary_indices_enable_bulk_filtering = 1;
SET use_skip_indexes_on_data_read = 0;
-- The bulk and scalar variants run the same predicates; the query condition cache key
-- does not include `use_minmax_index_bulk_filtering`, so with the (randomized) cache
-- enabled a later query could reuse the earlier variant's skip-index result instead
-- of exercising its own path.
SET use_query_condition_cache = 0;

-- 1. Decimal64(1) index, higher-scale Decimal64(2) bound.
-- Granule holds only 33.3; the scalar path keeps it for `d < 33.33` because 33.3 < 33.33, but a
-- lossy conversion of 33.33 down to Decimal64(1) yields 33.3 and `less(min = 33.3, 33.3)` is false.
DROP TABLE IF EXISTS t_minmax_decimal;

CREATE TABLE t_minmax_decimal
(
    d Decimal64(1),
    INDEX idx_d d TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO t_minmax_decimal VALUES (33.3), (33.3), (33.3), (33.3);
INSERT INTO t_minmax_decimal VALUES (100.0), (101.0), (102.0), (103.0);

SELECT 'decimal lower-scale index, higher-scale bound parity',
    length(groupUniqArray(c)) = 1 AS all_equal,
    any(c) AS count
FROM
(
    SELECT count() AS c FROM t_minmax_decimal WHERE d < toDecimal64('33.33', 2)
    SETTINGS use_minmax_index_bulk_filtering = 0
    UNION ALL
    SELECT count() AS c FROM t_minmax_decimal WHERE d < toDecimal64('33.33', 2)
    SETTINGS use_minmax_index_bulk_filtering = 1
);

DROP TABLE t_minmax_decimal;

-- 2. DateTime64(3) index, higher-scale DateTime64(6) bound.
-- Granule holds only `...00.123`; the scalar path keeps it for `t < ...00.123456`, but a lossy
-- conversion of the bound down to DateTime64(3) yields `...00.123` and `less(min, min)` is false.
DROP TABLE IF EXISTS t_minmax_dt64;

CREATE TABLE t_minmax_dt64
(
    t DateTime64(3),
    INDEX idx_t t TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO t_minmax_dt64 VALUES ('2024-01-01 00:00:00.123'), ('2024-01-01 00:00:00.123'), ('2024-01-01 00:00:00.123'), ('2024-01-01 00:00:00.123');
INSERT INTO t_minmax_dt64 VALUES ('2024-06-01 00:00:00.000'), ('2024-06-01 00:00:00.000'), ('2024-06-01 00:00:00.000'), ('2024-06-01 00:00:00.000');

SELECT 'datetime64 lower-scale index, higher-scale bound parity',
    length(groupUniqArray(c)) = 1 AS all_equal,
    any(c) AS count
FROM
(
    SELECT count() AS c FROM t_minmax_dt64 WHERE t < toDateTime64('2024-01-01 00:00:00.123456', 6)
    SETTINGS use_minmax_index_bulk_filtering = 0
    UNION ALL
    SELECT count() AS c FROM t_minmax_dt64 WHERE t < toDateTime64('2024-01-01 00:00:00.123456', 6)
    SETTINGS use_minmax_index_bulk_filtering = 1
);

DROP TABLE t_minmax_dt64;

-- 3. Float32 index, integer bound that is not representable as Float32.
-- 16777217 = 2^24 + 1 rounds down to 2^24 in Float32. The granule holds exactly 2^24, and the
-- scalar path keeps it for `f < 16777217` because 16777216 < 16777217, but converting the bound to
-- Float32 yields 16777216 and `less(min, min)` is false.
DROP TABLE IF EXISTS t_minmax_f32;

CREATE TABLE t_minmax_f32
(
    f Float32,
    INDEX idx_f f TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO t_minmax_f32 VALUES (16777216), (16777216), (16777216), (16777216);
INSERT INTO t_minmax_f32 VALUES (1e9), (1e9), (1e9), (1e9);

SELECT 'float32 index, non-representable integer bound parity',
    length(groupUniqArray(c)) = 1 AS all_equal,
    any(c) AS count
FROM
(
    SELECT count() AS c FROM t_minmax_f32 WHERE f < 16777217
    SETTINGS use_minmax_index_bulk_filtering = 0
    UNION ALL
    SELECT count() AS c FROM t_minmax_f32 WHERE f < 16777217
    SETTINGS use_minmax_index_bulk_filtering = 1
);

DROP TABLE t_minmax_f32;
