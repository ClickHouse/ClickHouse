-- Tags: zookeeper

SET allow_projection_column_list_in_replicated_metadata = 1;

DROP TABLE IF EXISTS t_untyped_codec_defaults_r1;
DROP TABLE IF EXISTS t_untyped_codec_defaults_r2;

-- `ZSTD` has a type-independent default level, so its omitted argument is canonicalized.
-- `Delta` has a type-dependent width, so its omitted argument must remain dynamic.
CREATE TABLE t_untyped_codec_defaults_r1
(
    k UInt64,
    x Int32,
    PROJECTION p (x CODEC(Delta, ZSTD)) AS (SELECT k, x ORDER BY k)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_untyped_codec_defaults', 'r1') ORDER BY k;

CREATE TABLE t_untyped_codec_defaults_r2
(
    k UInt64,
    x Int32,
    PROJECTION p (x CODEC(Delta, ZSTD(1))) AS (SELECT k, x ORDER BY k)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_untyped_codec_defaults', 'r2') ORDER BY k;

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table IN ('t_untyped_codec_defaults_r1', 't_untyped_codec_defaults_r2')
    AND codecs['x'] = 'CODEC(Delta(4), ZSTD(1))';

ALTER TABLE t_untyped_codec_defaults_r1 MODIFY COLUMN x Int64;
SYSTEM SYNC REPLICA t_untyped_codec_defaults_r2;

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table IN ('t_untyped_codec_defaults_r1', 't_untyped_codec_defaults_r2')
    AND codecs['x'] = 'CODEC(Delta(8), ZSTD(1))';

INSERT INTO t_untyped_codec_defaults_r1 SELECT number, number FROM numbers(10);
SYSTEM SYNC REPLICA t_untyped_codec_defaults_r2;
SELECT sum(x) FROM t_untyped_codec_defaults_r2
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p';

DROP TABLE t_untyped_codec_defaults_r1;
DROP TABLE t_untyped_codec_defaults_r2;

DROP TABLE IF EXISTS t_untyped_t64_defaults_r1;
DROP TABLE IF EXISTS t_untyped_t64_defaults_r2;

-- `T64` uses the column type internally, but an explicit `byte` variant and its default
-- have the same description for every supported type.
CREATE TABLE t_untyped_t64_defaults_r1
(
    k UInt64,
    x Int32,
    PROJECTION p (x CODEC(T64)) AS (SELECT k, x ORDER BY k)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_untyped_t64_defaults', 'r1') ORDER BY k;

CREATE TABLE t_untyped_t64_defaults_r2
(
    k UInt64,
    x Int32,
    PROJECTION p (x CODEC(T64('byte'))) AS (SELECT k, x ORDER BY k)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_untyped_t64_defaults', 'r2') ORDER BY k;

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table IN ('t_untyped_t64_defaults_r1', 't_untyped_t64_defaults_r2')
    AND codecs['x'] = 'CODEC(T64)';

DROP TABLE t_untyped_t64_defaults_r1;
DROP TABLE t_untyped_t64_defaults_r2;

SET enable_alp_codec = 1;

DROP TABLE IF EXISTS t_untyped_alp_defaults_r1;
DROP TABLE IF EXISTS t_untyped_alp_defaults_r2;

-- `ALP` and `ALP(AUTO)` take the same path for either floating-point width.
CREATE TABLE t_untyped_alp_defaults_r1
(
    k UInt64,
    x Float32,
    PROJECTION p (x CODEC(ALP)) AS (SELECT k, x ORDER BY k)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_untyped_alp_defaults', 'r1') ORDER BY k;

CREATE TABLE t_untyped_alp_defaults_r2
(
    k UInt64,
    x Float32,
    PROJECTION p (x CODEC(ALP(AUTO))) AS (SELECT k, x ORDER BY k)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_untyped_alp_defaults', 'r2') ORDER BY k;

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table IN ('t_untyped_alp_defaults_r1', 't_untyped_alp_defaults_r2')
    AND codecs['x'] = 'CODEC(ALP)';

ALTER TABLE t_untyped_alp_defaults_r1 MODIFY COLUMN x Float64;
SYSTEM SYNC REPLICA t_untyped_alp_defaults_r2;

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table IN ('t_untyped_alp_defaults_r1', 't_untyped_alp_defaults_r2')
    AND codecs['x'] = 'CODEC(ALP)';

DROP TABLE t_untyped_alp_defaults_r1;
DROP TABLE t_untyped_alp_defaults_r2;

DROP TABLE IF EXISTS t_untyped_fpc_defaults_r1;
DROP TABLE IF EXISTS t_untyped_fpc_defaults_r2;

-- `FPC` defaults to level 12 independently of the type, but its optional second argument
-- pins a width that would otherwise follow the type. Normalize only the omitted level.
CREATE TABLE t_untyped_fpc_defaults_r1
(
    k UInt64,
    x Float32,
    PROJECTION p (x CODEC(FPC)) AS (SELECT k, x ORDER BY k)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_untyped_fpc_defaults', 'r1') ORDER BY k;

CREATE TABLE t_untyped_fpc_defaults_r2
(
    k UInt64,
    x Float32,
    PROJECTION p (x CODEC(FPC(12, 4))) AS (SELECT k, x ORDER BY k)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_untyped_fpc_defaults', 'r2') ORDER BY k; -- { serverError METADATA_MISMATCH }

CREATE TABLE t_untyped_fpc_defaults_r2
(
    k UInt64,
    x Float32,
    PROJECTION p (x CODEC(FPC(12))) AS (SELECT k, x ORDER BY k)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_untyped_fpc_defaults', 'r2') ORDER BY k;

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table IN ('t_untyped_fpc_defaults_r1', 't_untyped_fpc_defaults_r2')
    AND codecs['x'] = 'CODEC(FPC(12))';

ALTER TABLE t_untyped_fpc_defaults_r1 MODIFY COLUMN x Float64;
SYSTEM SYNC REPLICA t_untyped_fpc_defaults_r2;

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table IN ('t_untyped_fpc_defaults_r1', 't_untyped_fpc_defaults_r2')
    AND codecs['x'] = 'CODEC(FPC(12))';

DROP TABLE t_untyped_fpc_defaults_r1;
DROP TABLE t_untyped_fpc_defaults_r2;

-- A supplied FPC width must survive preprocessing into the projection's part-writer metadata,
-- including when the SELECT output type later changes.
CREATE TABLE t_projection_fpc_pinned_width
(
    k UInt64,
    x Float32,
    PROJECTION p (x CODEC(FPC(12, 4))) AS (SELECT k, x ORDER BY k)
)
ENGINE = MergeTree ORDER BY k;

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_fpc_pinned_width'
    AND codecs['x'] = 'CODEC(FPC(12, 4))';

ALTER TABLE t_projection_fpc_pinned_width MODIFY COLUMN x Float64;

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_fpc_pinned_width'
    AND codecs['x'] = 'CODEC(FPC(12, 4))';

INSERT INTO t_projection_fpc_pinned_width SELECT number, toFloat64(number) FROM numbers(10);
SELECT sum(x) FROM t_projection_fpc_pinned_width
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p';

DROP TABLE t_projection_fpc_pinned_width;
