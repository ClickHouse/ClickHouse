-- Tags: no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings

SET optimize_read_in_order = 0;

DROP TABLE IF EXISTS t_projection_codec_alias;
DROP TABLE IF EXISTS t_projection_codec_alias_copy;
DROP TABLE IF EXISTS t_projection_codec_mixed_expression;
DROP TABLE IF EXISTS t_projection_codec_aggregate_alias;
DROP TABLE IF EXISTS t_projection_codec_legacy_name;
DROP TABLE IF EXISTS t_projection_codec_star;
DROP TABLE IF EXISTS t_projection_codec_bad;

CREATE TABLE t_projection_codec_alias
(
    k UInt64,
    v UInt64,
    PROJECTION p (bump CODEC(NONE)) AS (SELECT k, v AS bump ORDER BY k)
)
ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_projection_codec_alias SELECT number, number FROM numbers(1000);
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_alias' AND name = 'p'
    AND active AND column != 'bump' AND type = 'UInt64'
    AND column_data_compressed_bytes >= column_data_uncompressed_bytes;
SELECT k, v + 1 FROM t_projection_codec_alias WHERE k < 3 ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

CREATE TABLE t_projection_codec_alias_copy AS t_projection_codec_alias;
INSERT INTO t_projection_codec_alias_copy SELECT k, v FROM t_projection_codec_alias WHERE k < 3;
SELECT position(create_table_query, '`bump` CODEC(NONE)') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_codec_alias_copy';
SELECT k, v FROM t_projection_codec_alias_copy ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

-- A calculated output without a codec does not prevent a stored output from using one.
CREATE TABLE t_projection_codec_mixed_expression
(
    k UInt64,
    v UInt64,
    PROJECTION p (k CODEC(NONE)) AS (SELECT k, v + 1 AS bump ORDER BY k)
)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_projection_codec_mixed_expression VALUES (1, 2);
SELECT k, v + 1 FROM t_projection_codec_mixed_expression
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT position(create_table_query, '`bump` CODEC(NONE)') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_codec_alias';
DETACH TABLE t_projection_codec_alias;
ATTACH TABLE t_projection_codec_alias;
SELECT k, v + 1 FROM t_projection_codec_alias WHERE k < 3 ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

CREATE TABLE t_projection_codec_aggregate_alias
(
    k UInt64,
    v UInt64,
    PROJECTION p (s CODEC(ZSTD)) AS (SELECT k, sum(v) AS s GROUP BY k)
)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_projection_codec_aggregate_alias SELECT number % 10, number FROM numbers(1000);
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_aggregate_alias' AND name = 'p'
    AND active AND column != 's' AND column LIKE 'sum(%';
SELECT k, sum(v) FROM t_projection_codec_aggregate_alias GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT position(create_table_query, '`s` CODEC(ZSTD)') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_codec_aggregate_alias';
DETACH TABLE t_projection_codec_aggregate_alias;
ATTACH TABLE t_projection_codec_aggregate_alias;
SELECT sum(v) FROM t_projection_codec_aggregate_alias
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

-- A pre-existing declaration can still refer to the expression-derived part name.
CREATE TABLE t_projection_codec_legacy_name
(
    k UInt64,
    v UInt64,
    PROJECTION p (`sum(v)` CODEC(ZSTD)) AS (SELECT k, sum(v) AS s GROUP BY k)
)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_projection_codec_legacy_name SELECT number % 10, number FROM numbers(100);
DETACH TABLE t_projection_codec_legacy_name;
ATTACH TABLE t_projection_codec_legacy_name;
SELECT sum(v) FROM t_projection_codec_legacy_name
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

-- Star expansion exposes ordinary source columns to the declaration.
CREATE TABLE t_projection_codec_star
(
    k UInt64,
    v UInt64,
    PROJECTION p (v CODEC(ZSTD)) AS (SELECT * ORDER BY k)
)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_projection_codec_star VALUES (1, 2);
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codec_star' AND name = 'p'
    AND active AND column = 'v';

CREATE TABLE t_projection_codec_bad
    (k UInt64, v UInt64, PROJECTION p (no_such_output CODEC(ZSTD)) AS (SELECT k, v ORDER BY k))
ENGINE = MergeTree ORDER BY k; -- { serverError THERE_IS_NO_COLUMN }
CREATE TABLE t_projection_codec_bad
    (k UInt64, v UInt64, PROJECTION p (v CODEC(ZSTD), v CODEC(LZ4)) AS (SELECT k, v ORDER BY k))
ENGINE = MergeTree ORDER BY k; -- { serverError DUPLICATE_COLUMN }
CREATE TABLE t_projection_codec_bad
    (k UInt64, v UInt64, PROJECTION p (k CODEC(ZSTD)) AS (SELECT v ORDER BY k))
ENGINE = MergeTree ORDER BY k; -- { serverError THERE_IS_NO_COLUMN }
CREATE TABLE t_projection_codec_bad
    (k UInt64, v UInt64, PROJECTION p (bump CODEC(ZSTD)) AS (SELECT v + 1 AS bump ORDER BY k))
ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_projection_codec_bad
    (k UInt64, v UInt64, PROJECTION p (bump CODEC(ZSTD), v CODEC(LZ4)) AS (SELECT v AS bump ORDER BY k))
ENGINE = MergeTree ORDER BY k; -- { serverError DUPLICATE_COLUMN }
CREATE TABLE t_projection_codec_bad
    (k UInt64, v UInt64, PROJECTION p (k CODEC(ZSTD)) AS (SELECT v AS k, k AS key ORDER BY key))
ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_projection_codec_bad
    (k UInt64, v UInt64, PROJECTION p (x CODEC(ZSTD)) AS (SELECT v, v AS x ORDER BY k))
ENGINE = MergeTree ORDER BY k; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_codec_bad';

DROP TABLE t_projection_codec_star;
DROP TABLE t_projection_codec_legacy_name;
DROP TABLE t_projection_codec_aggregate_alias;
DROP TABLE t_projection_codec_alias;
DROP TABLE t_projection_codec_alias_copy;
DROP TABLE t_projection_codec_mixed_expression;
