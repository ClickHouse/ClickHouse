-- Tags: no-parallel-replicas
-- no-parallel-replicas: per-query SETTINGS toggling skip-index evaluation paths
-- must take effect on the executing replica.

-- The test runner randomizes `secondary_indices_enable_bulk_filtering` and
-- `use_skip_indexes_on_data_read`. Both structurally affect where and whether bulk
-- filtering runs:
--   * `secondary_indices_enable_bulk_filtering = 0` blanket-disables the bulk path.
--   * `use_skip_indexes_on_data_read = 1` moves skip-index evaluation out of
--     `filterMarksUsingIndex` (where our bulk hook lives) and into data-read time.
-- Pin both so the parity checks below actually exercise bulk.
SET secondary_indices_enable_bulk_filtering = 1;
SET use_skip_indexes_on_data_read = 0;

-- The bulk-filtering path must return the same answer as the generic per-granule path
-- for every RPN shape the minmax skip index supports, for every numeric column type,
-- for Nullable / Enum (which fall back to the generic path), and regardless of whether
-- JIT (`compile_expressions`) is on or off.

DROP TABLE IF EXISTS t_bulk_num;
DROP TABLE IF EXISTS t_bulk_nullable;
DROP TABLE IF EXISTS t_bulk_enum;

CREATE TABLE t_bulk_num
(
    i32 Int32, u32 UInt32, i64 Int64, u64 UInt64, f64 Float64, d Date, dt DateTime,
    INDEX idx_i32 i32 TYPE minmax GRANULARITY 1,
    INDEX idx_u32 u32 TYPE minmax GRANULARITY 1,
    INDEX idx_i64 i64 TYPE minmax GRANULARITY 1,
    INDEX idx_u64 u64 TYPE minmax GRANULARITY 1,
    INDEX idx_f64 f64 TYPE minmax GRANULARITY 1,
    INDEX idx_d d TYPE minmax GRANULARITY 1,
    INDEX idx_dt dt TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 128;

INSERT INTO t_bulk_num
SELECT number - 5000 AS i32, number AS u32, toInt64(number) - 5000 AS i64,
       toUInt64(number) AS u64, toFloat64(number) / 10.0 AS f64,
       toDate('2024-01-01') + (number % 365) AS d,
       toDateTime('2024-01-01 00:00:00') + number AS dt
FROM numbers(20000);

-- Run each query under bulk_filtering = 0 then = 1; expect identical counts.
-- jit_bundle runs compile_expressions = 1; plain runs = 0. Both must agree with the
-- bulk = 0 reference.

SELECT 'i32 range',
    (SELECT count() FROM t_bulk_num WHERE i32 BETWEEN -100 AND 100
         SETTINGS use_minmax_index_bulk_filtering = 0, compile_expressions = 0) =
    (SELECT count() FROM t_bulk_num WHERE i32 BETWEEN -100 AND 100
         SETTINGS use_minmax_index_bulk_filtering = 1, compile_expressions = 0) AS no_jit,
    (SELECT count() FROM t_bulk_num WHERE i32 BETWEEN -100 AND 100
         SETTINGS use_minmax_index_bulk_filtering = 0, compile_expressions = 0) =
    (SELECT count() FROM t_bulk_num WHERE i32 BETWEEN -100 AND 100
         SETTINGS use_minmax_index_bulk_filtering = 1, compile_expressions = 1,
                  min_count_to_compile_expression = 1) AS jit;

SELECT 'u32 equality',
    (SELECT count() FROM t_bulk_num WHERE u32 = 12345
         SETTINGS use_minmax_index_bulk_filtering = 0) =
    (SELECT count() FROM t_bulk_num WHERE u32 = 12345
         SETTINGS use_minmax_index_bulk_filtering = 1) AS eq;

SELECT 'f64 OR',
    (SELECT count() FROM t_bulk_num WHERE f64 < 10.0 OR f64 > 1990.0
         SETTINGS use_minmax_index_bulk_filtering = 0) =
    (SELECT count() FROM t_bulk_num WHERE f64 < 10.0 OR f64 > 1990.0
         SETTINGS use_minmax_index_bulk_filtering = 1) AS eq;

SELECT 'NOT IN RANGE (via NOT BETWEEN)',
    (SELECT count() FROM t_bulk_num WHERE NOT (i64 BETWEEN 0 AND 5000)
         SETTINGS use_minmax_index_bulk_filtering = 0) =
    (SELECT count() FROM t_bulk_num WHERE NOT (i64 BETWEEN 0 AND 5000)
         SETTINGS use_minmax_index_bulk_filtering = 1) AS eq;

SELECT 'AND of two columns',
    (SELECT count() FROM t_bulk_num WHERE u64 > 10000 AND f64 < 500.0
         SETTINGS use_minmax_index_bulk_filtering = 0) =
    (SELECT count() FROM t_bulk_num WHERE u64 > 10000 AND f64 < 500.0
         SETTINGS use_minmax_index_bulk_filtering = 1) AS eq;

SELECT 'Date range',
    (SELECT count() FROM t_bulk_num WHERE d BETWEEN toDate('2024-03-01') AND toDate('2024-06-30')
         SETTINGS use_minmax_index_bulk_filtering = 0) =
    (SELECT count() FROM t_bulk_num WHERE d BETWEEN toDate('2024-03-01') AND toDate('2024-06-30')
         SETTINGS use_minmax_index_bulk_filtering = 1) AS eq;

SELECT 'DateTime range',
    (SELECT count() FROM t_bulk_num WHERE dt >= toDateTime('2024-01-01 01:00:00')
                                     AND dt < toDateTime('2024-01-01 02:00:00')
         SETTINGS use_minmax_index_bulk_filtering = 0) =
    (SELECT count() FROM t_bulk_num WHERE dt >= toDateTime('2024-01-01 01:00:00')
                                     AND dt < toDateTime('2024-01-01 02:00:00')
         SETTINGS use_minmax_index_bulk_filtering = 1) AS eq;

-- Nullable column: bulk path must refuse the DAG and fall back to the per-granule
-- evaluator. Any answer mismatch here would mean the fallback is broken.
CREATE TABLE t_bulk_nullable
(
    x Nullable(Int32),
    INDEX idx_x x TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 128, allow_nullable_key = 1;

INSERT INTO t_bulk_nullable
SELECT if(number % 7 = 0, NULL, toInt32(number) - 5000)
FROM numbers(5000);

SELECT 'Nullable, fallback path',
    (SELECT count() FROM t_bulk_nullable WHERE x BETWEEN -100 AND 100
         SETTINGS use_minmax_index_bulk_filtering = 0) =
    (SELECT count() FROM t_bulk_nullable WHERE x BETWEEN -100 AND 100
         SETTINGS use_minmax_index_bulk_filtering = 1) AS eq;

-- Enum8 column: the bulk deserializer used to miscategorize Enum8 under FastKind::U8
-- (matching the tag `isUInt8`), which threw at `assert_cast<ColumnVector<UInt8>>` since
-- the column is actually `ColumnVector<Int8>`. Verify the classifier routes Enum8 to the
-- I8 path and produces a correct answer.
CREATE TABLE t_bulk_enum
(
    e Enum8('a' = 1, 'b' = 2, 'c' = 3),
    INDEX idx_e e TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 128;

INSERT INTO t_bulk_enum
SELECT [cast('a' AS Enum8('a' = 1, 'b' = 2, 'c' = 3)),
        cast('b' AS Enum8('a' = 1, 'b' = 2, 'c' = 3)),
        cast('c' AS Enum8('a' = 1, 'b' = 2, 'c' = 3))][1 + (number % 3)]
FROM numbers(5000);

SELECT 'Enum8',
    (SELECT count() FROM t_bulk_enum WHERE e = 'b'
         SETTINGS use_minmax_index_bulk_filtering = 0) =
    (SELECT count() FROM t_bulk_enum WHERE e = 'b'
         SETTINGS use_minmax_index_bulk_filtering = 1) AS eq;

-- Float NaN granules. A granule that mixes finite values with NaN is stored as
-- [finite_min, NaN] because NaN sorts last. A lower-bounded predicate (>=, BETWEEN)
-- must keep such a granule: the finite values may still match. The bulk DAG path
-- computes `intersects_lower` as `greaterOrEquals(max, left)`, and `greaterOrEquals(NaN, left)`
-- is false, so without mirroring KeyCondition's NaN semantics it would wrongly prune the
-- granule and undercount. Each granule below gets a NaN in its first row, so every
-- granule's stored max is NaN.
CREATE TABLE t_bulk_nan
(
    f Float64,
    INDEX idx_f f TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 128;

INSERT INTO t_bulk_nan
SELECT if(number % 128 = 0, nan, toFloat64(number))
FROM numbers(2048);

SELECT 'f64 NaN granule, lower bound',
    (SELECT count() FROM t_bulk_nan WHERE f >= 1000.0
         SETTINGS use_minmax_index_bulk_filtering = 0) =
    (SELECT count() FROM t_bulk_nan WHERE f >= 1000.0
         SETTINGS use_minmax_index_bulk_filtering = 1) AS eq;

SELECT 'f64 NaN granule, BETWEEN',
    (SELECT count() FROM t_bulk_nan WHERE f BETWEEN 1000.0 AND 1500.0
         SETTINGS use_minmax_index_bulk_filtering = 0) =
    (SELECT count() FROM t_bulk_nan WHERE f BETWEEN 1000.0 AND 1500.0
         SETTINGS use_minmax_index_bulk_filtering = 1) AS eq;

-- Disjunction interaction. With `use_skip_indexes_for_disjunctions = 1` active and
-- more than one useful index, the runtime disjunction-merge path runs. The bulk path
-- doesn't populate the partial-disjunction bitset, so it is only used when the
-- projected per-index condition is a pure conjunction; when the per-index condition
-- itself contains an OR, the code falls back to the scalar per-granule path for that
-- index. Either way the final answer must match the non-bulk reference.
CREATE TABLE t_bulk_disjunction
(
    t DateTime,
    v UInt32,
    INDEX idx_t t TYPE minmax GRANULARITY 1,
    INDEX idx_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY v
SETTINGS index_granularity = 128;

INSERT INTO t_bulk_disjunction
SELECT toDateTime('2024-01-01 00:00:00') + number * 60, number
FROM numbers(20000);

-- Observability shape: time AND (OR over v). Each index's projected condition is
-- AND-only, so bulk on idx_t stays eligible under the disjunction setting.
SELECT 'bulk observability parity',
    length(groupUniqArray(c)) = 1 AS all_equal,
    any(c) AS count
FROM
(
    SELECT count() AS c FROM t_bulk_disjunction
    WHERE t >= '2024-01-01 00:00:00' AND t < '2024-01-10 00:00:00' AND (v < 100 OR v > 19900)
    SETTINGS use_minmax_index_bulk_filtering = 0, use_skip_indexes_for_disjunctions = 0
    UNION ALL
    SELECT count() AS c FROM t_bulk_disjunction
    WHERE t >= '2024-01-01 00:00:00' AND t < '2024-01-10 00:00:00' AND (v < 100 OR v > 19900)
    SETTINGS use_minmax_index_bulk_filtering = 0, use_skip_indexes_for_disjunctions = 1
    UNION ALL
    SELECT count() AS c FROM t_bulk_disjunction
    WHERE t >= '2024-01-01 00:00:00' AND t < '2024-01-10 00:00:00' AND (v < 100 OR v > 19900)
    SETTINGS use_minmax_index_bulk_filtering = 1, use_skip_indexes_for_disjunctions = 0
    UNION ALL
    SELECT count() AS c FROM t_bulk_disjunction
    WHERE t >= '2024-01-01 00:00:00' AND t < '2024-01-10 00:00:00' AND (v < 100 OR v > 19900)
    SETTINGS use_minmax_index_bulk_filtering = 1, use_skip_indexes_for_disjunctions = 1
);

-- Per-index disjunction: idx_v's projected condition has an OR directly on v, which
-- defeats bulk's precision-preserving criterion. The bulk path must fall back to the
-- scalar per-granule path for that index, and the final answer must still match.
SELECT 'bulk own-disjunction parity',
    length(groupUniqArray(c)) = 1 AS all_equal,
    any(c) AS count
FROM
(
    SELECT count() AS c FROM t_bulk_disjunction
    WHERE (v < 1000 OR v > 19000) AND t < '2024-01-15 00:00:00'
    SETTINGS use_minmax_index_bulk_filtering = 0, use_skip_indexes_for_disjunctions = 0
    UNION ALL
    SELECT count() AS c FROM t_bulk_disjunction
    WHERE (v < 1000 OR v > 19000) AND t < '2024-01-15 00:00:00'
    SETTINGS use_minmax_index_bulk_filtering = 0, use_skip_indexes_for_disjunctions = 1
    UNION ALL
    SELECT count() AS c FROM t_bulk_disjunction
    WHERE (v < 1000 OR v > 19000) AND t < '2024-01-15 00:00:00'
    SETTINGS use_minmax_index_bulk_filtering = 1, use_skip_indexes_for_disjunctions = 0
    UNION ALL
    SELECT count() AS c FROM t_bulk_disjunction
    WHERE (v < 1000 OR v > 19000) AND t < '2024-01-15 00:00:00'
    SETTINGS use_minmax_index_bulk_filtering = 1, use_skip_indexes_for_disjunctions = 1
);

DROP TABLE t_bulk_num;
DROP TABLE t_bulk_nullable;
DROP TABLE t_bulk_enum;
DROP TABLE t_bulk_nan;
DROP TABLE t_bulk_disjunction;
