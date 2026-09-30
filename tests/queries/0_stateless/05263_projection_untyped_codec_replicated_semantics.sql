-- Tags: zookeeper

SET allow_projection_column_list_in_replicated_metadata = 1;

DROP TABLE IF EXISTS t_untyped_codec_r1;
DROP TABLE IF EXISTS t_untyped_codec_r2;

-- The omitted type and omitted `Delta` argument must remain omitted in replicated metadata:
-- after widening, `Delta` resolves again while an explicit `Delta(4)` stays fixed.
CREATE TABLE t_untyped_codec_r1
(
    k UInt64,
    x Int32,
    PROJECTION p (x CODEC(Delta, ZSTD(1))) AS (SELECT k, x ORDER BY k)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_untyped_codec', 'r1') ORDER BY k;

CREATE TABLE t_untyped_codec_r2
(
    k UInt64,
    x Int32,
    PROJECTION p (x CODEC(Delta(4), ZSTD(1))) AS (SELECT k, x ORDER BY k)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_untyped_codec', 'r2') ORDER BY k; -- { serverError METADATA_MISMATCH }

-- The matching dynamic declaration can join the table and change with its type.
CREATE TABLE t_untyped_codec_r2
(
    k UInt64,
    x Int32,
    PROJECTION p (x CODEC(Delta, ZSTD(1))) AS (SELECT k, x ORDER BY k)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_untyped_codec', 'r2') ORDER BY k;

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table IN ('t_untyped_codec_r1', 't_untyped_codec_r2')
    AND codecs['x'] = 'CODEC(Delta(4), ZSTD(1))';

-- Both replicas infer the new type's eight-byte `Delta` width.
ALTER TABLE t_untyped_codec_r1 MODIFY COLUMN x Int64;
SYSTEM SYNC REPLICA t_untyped_codec_r2;

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table IN ('t_untyped_codec_r1', 't_untyped_codec_r2')
    AND codecs['x'] = 'CODEC(Delta(8), ZSTD(1))';

INSERT INTO t_untyped_codec_r1 SELECT number, number FROM numbers(100);
SYSTEM SYNC REPLICA t_untyped_codec_r2;
SELECT sum(x) FROM t_untyped_codec_r2
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p';

DETACH TABLE t_untyped_codec_r2;
ATTACH TABLE t_untyped_codec_r2;
SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table = 't_untyped_codec_r2'
    AND codecs['x'] = 'CODEC(Delta(8), ZSTD(1))';

DROP TABLE t_untyped_codec_r1;
DROP TABLE t_untyped_codec_r2;

DROP TABLE IF EXISTS t_untyped_double_delta_r1;
DROP TABLE IF EXISTS t_untyped_double_delta_r2;

-- Omitted DoubleDelta width follows the output type, while an explicit width stays pinned.
-- They happen to match for UInt32, but must remain distinct in replicated metadata.
CREATE TABLE t_untyped_double_delta_r1
(
    k UInt64,
    x UInt32,
    PROJECTION p (x CODEC(DoubleDelta)) AS (SELECT k, x ORDER BY k)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_untyped_double_delta', 'r1') ORDER BY k;

CREATE TABLE t_untyped_double_delta_r2
(
    k UInt64,
    x UInt32,
    PROJECTION p (x CODEC(DoubleDelta(4))) AS (SELECT k, x ORDER BY k)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_untyped_double_delta', 'r2') ORDER BY k; -- { serverError METADATA_MISMATCH }

CREATE TABLE t_untyped_double_delta_r2
(
    k UInt64,
    x UInt32,
    PROJECTION p (x CODEC(DoubleDelta)) AS (SELECT k, x ORDER BY k)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_untyped_double_delta', 'r2') ORDER BY k;

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table IN ('t_untyped_double_delta_r1', 't_untyped_double_delta_r2')
    AND codecs['x'] = 'CODEC(DoubleDelta)';

ALTER TABLE t_untyped_double_delta_r1 MODIFY COLUMN x UInt64;
SYSTEM SYNC REPLICA t_untyped_double_delta_r2;

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table IN ('t_untyped_double_delta_r1', 't_untyped_double_delta_r2')
    AND codecs['x'] = 'CODEC(DoubleDelta)';

DROP TABLE t_untyped_double_delta_r1;
DROP TABLE t_untyped_double_delta_r2;

CREATE TABLE t_pinned_double_delta
(
    k UInt64,
    x UInt32,
    PROJECTION p (x CODEC(DoubleDelta(4))) AS (SELECT k, x ORDER BY k)
)
ENGINE = MergeTree ORDER BY k;

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table = 't_pinned_double_delta'
    AND codecs['x'] = 'CODEC(DoubleDelta(4))';

ALTER TABLE t_pinned_double_delta MODIFY COLUMN x UInt64;

SELECT count() FROM system.projections
WHERE database = currentDatabase() AND table = 't_pinned_double_delta'
    AND codecs['x'] = 'CODEC(DoubleDelta(4))';

INSERT INTO t_pinned_double_delta SELECT number, number FROM numbers(10);
SELECT sum(x) FROM t_pinned_double_delta
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p';

DROP TABLE t_pinned_double_delta;
