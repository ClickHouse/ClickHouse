-- Tags: no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_projection_codecs;
DROP TABLE IF EXISTS t_projection_codecs_copy;
DROP TABLE IF EXISTS t_projection_codecs_aggregate;

CREATE TABLE t_projection_codecs
(
    k UInt64,
    ts UInt64,
    PROJECTION p (k CODEC(NONE), ts CODEC(DoubleDelta, ZSTD)) AS (SELECT k, ts ORDER BY ts)
)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, default_compression_codec = 'LZ4';

INSERT INTO t_projection_codecs SELECT number, number FROM numbers(100000);

-- The override reaches the projection writer and does not affect the parent column.
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codecs' AND name = 'p'
    AND active AND column = 'k' AND column_data_compressed_bytes > column_data_uncompressed_bytes;
SELECT count() = 1 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codecs'
    AND active AND column = 'k' AND column_data_compressed_bytes < column_data_uncompressed_bytes;

SELECT position(create_table_query, 'CODEC(NONE)') > 0
    AND position(create_table_query, 'CODEC(DoubleDelta, ZSTD)') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_codecs';

DETACH TABLE t_projection_codecs;
ATTACH TABLE t_projection_codecs;
SELECT position(create_table_query, 'CODEC(DoubleDelta, ZSTD)') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_codecs';
SELECT sum(ts) = 4999950000 FROM t_projection_codecs;

CREATE TABLE t_projection_codecs_copy AS t_projection_codecs;
SELECT position(create_table_query, 'CODEC(DoubleDelta, ZSTD)') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_codecs_copy';
INSERT INTO t_projection_codecs_copy SELECT k, ts FROM t_projection_codecs WHERE k < 1000;
SELECT count() = 1000 FROM t_projection_codecs_copy;
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codecs_copy' AND name = 'p'
    AND active AND column = 'k' AND column_data_compressed_bytes > column_data_uncompressed_bytes;

CREATE TABLE t_projection_codecs_aggregate
(
    k UInt64,
    v UInt64,
    PROJECTION p (k CODEC(NONE)) AS (SELECT k, sum(v) GROUP BY k)
)
ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_projection_codecs_aggregate SELECT number % 10, number FROM numbers(10000);
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codecs_aggregate' AND name = 'p'
    AND active AND column = 'k' AND column_data_compressed_bytes > column_data_uncompressed_bytes;
SELECT sum(v) = 49995000 FROM t_projection_codecs_aggregate;

DROP TABLE t_projection_codecs_aggregate;
DROP TABLE t_projection_codecs_copy;
DROP TABLE t_projection_codecs;
