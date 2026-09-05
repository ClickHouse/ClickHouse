-- Tags: no-fasttest
-- no-fasttest: needs the SZ3 library

-- `ALTER TABLE ... RECOMPRESS COLUMN` with a lossy codec (`SZ3`) changes the values stored in the part,
-- while the mutation hardlinks the part's projections and skip indices unchanged. They would keep
-- describing the pre-recompression values, so base-part and projection reads could disagree and a stale
-- skip index could prune granules that do match. `MergeTreeData::checkMutationIsPossible` rejects the
-- combination; a lossless codec, and a lossy codec no projection or index depends on, stay allowed.

SET allow_experimental_codecs = 1;
SET mutations_sync = 2;
SET check_query_single_value_result = 1;

-- Full part storage is pinned (`min_bytes_for_full_part_storage` may be randomized in tests): a packed
-- part does not support in-place recompression, so `RECOMPRESS COLUMN key` would rewrite the part as a
-- whole, re-serialize `val` with its lossy codec, and be rejected because the projection reads `val`.
-- The in-place path this test exercises requires a wide part with full storage.
DROP TABLE IF EXISTS t_recompress_lossy_projection;
CREATE TABLE t_recompress_lossy_projection
(
    key UInt64,
    val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    PROJECTION p (SELECT key, val ORDER BY val)
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0;

INSERT INTO t_recompress_lossy_projection SELECT number, sin(number / 1000.) * 100 FROM numbers(1000);

ALTER TABLE t_recompress_lossy_projection RECOMPRESS COLUMN val; -- { serverError SUPPORT_IS_DISABLED }

-- A column the projection does not read can still be recompressed.
ALTER TABLE t_recompress_lossy_projection MODIFY COLUMN key UInt64 CODEC(ZSTD(1));
ALTER TABLE t_recompress_lossy_projection RECOMPRESS COLUMN key;
SELECT 'projection table, non-dependent column recompressed', count() FROM t_recompress_lossy_projection;

DROP TABLE t_recompress_lossy_projection;

DROP TABLE IF EXISTS t_recompress_lossy_index;
CREATE TABLE t_recompress_lossy_index
(
    key UInt64,
    val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    INDEX idx val TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_index SELECT number, sin(number / 1000.) * 100 FROM numbers(1000);

ALTER TABLE t_recompress_lossy_index RECOMPRESS COLUMN val; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_recompress_lossy_index;

-- An index over an expression of the column is rejected as well.
DROP TABLE IF EXISTS t_recompress_lossy_index_expr;
CREATE TABLE t_recompress_lossy_index_expr
(
    key UInt64,
    val Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    INDEX idx floor(val) TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossy_index_expr SELECT number, sin(number / 1000.) * 100 FROM numbers(1000);

ALTER TABLE t_recompress_lossy_index_expr RECOMPRESS COLUMN val; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_recompress_lossy_index_expr;

-- Control: with a lossless codec the values are preserved, so dependent projections and indices stay
-- valid and the recompression is allowed.
DROP TABLE IF EXISTS t_recompress_lossless_dependents;
CREATE TABLE t_recompress_lossless_dependents
(
    key UInt64,
    orig Float64,
    val Float64 CODEC(LZ4),
    INDEX idx val TYPE minmax GRANULARITY 1,
    PROJECTION p (SELECT key, val ORDER BY val)
)
ENGINE = MergeTree ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_recompress_lossless_dependents
SELECT number, sin(number / 1000.) * 100, sin(number / 1000.) * 100 FROM numbers(1000);

ALTER TABLE t_recompress_lossless_dependents MODIFY COLUMN val Float64 CODEC(ZSTD(3));
ALTER TABLE t_recompress_lossless_dependents RECOMPRESS COLUMN val;

-- The values are bit-exact after a lossless recompression, so the projection and the index stay valid.
SELECT 'lossless dependents recompressed', count(), sum(val != orig) FROM t_recompress_lossless_dependents;
SELECT 'lossless dependents projection agrees', count() FROM t_recompress_lossless_dependents WHERE val > 0 SETTINGS optimize_use_projections = 1;
CHECK TABLE t_recompress_lossless_dependents;

DROP TABLE t_recompress_lossless_dependents;
