-- Tags: no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_projection_codecs_alter;
CREATE TABLE t_projection_codecs_alter (k UInt64, ts UInt64)
ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_projection_codecs_alter SELECT number, number FROM numbers(100000);

ALTER TABLE t_projection_codecs_alter ADD PROJECTION p
    (k CODEC(NONE), ts CODEC(DoubleDelta, ZSTD)) AS (SELECT k, ts ORDER BY k);
ALTER TABLE t_projection_codecs_alter MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codecs_alter' AND name = 'p'
    AND active AND column = 'k' AND column_data_compressed_bytes > column_data_uncompressed_bytes;

-- Omitted codec widths follow a valid change to the SELECT output type.
ALTER TABLE t_projection_codecs_alter MODIFY COLUMN ts UInt32 SETTINGS mutations_sync = 2;
SELECT position(create_table_query, 'CODEC(DoubleDelta, ZSTD)') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_codecs_alter';

-- An incompatible output type must be rejected without changing published metadata.
ALTER TABLE t_projection_codecs_alter MODIFY COLUMN ts String; -- { serverError BAD_ARGUMENTS }
SELECT type = 'UInt32' FROM system.columns
WHERE database = currentDatabase() AND table = 't_projection_codecs_alter' AND name = 'ts';

ALTER TABLE t_projection_codecs_alter MODIFY PROJECTION p
    (k CODEC(NONE), ts CODEC(DoubleDelta, ZSTD)) AS (SELECT k, ts ORDER BY k)
    WITH SETTINGS (index_granularity = 128);
SELECT position(create_table_query, 'index_granularity = 128') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_codecs_alter';

INSERT INTO t_projection_codecs_alter SELECT number, number FROM numbers(100000, 100000);
OPTIMIZE TABLE t_projection_codecs_alter FINAL;
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codecs_alter' AND name = 'p'
    AND active AND column = 'k' AND column_data_compressed_bytes > column_data_uncompressed_bytes;

ALTER TABLE t_projection_codecs_alter UPDATE ts = ts + 1 WHERE k < 10 SETTINGS mutations_sync = 2;
SELECT sum(ts) - sum(k) FROM t_projection_codecs_alter;
SELECT count() = 1 FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_codecs_alter' AND name = 'p'
    AND active AND column = 'k' AND column_data_compressed_bytes > column_data_uncompressed_bytes;

DROP TABLE t_projection_codecs_alter;
