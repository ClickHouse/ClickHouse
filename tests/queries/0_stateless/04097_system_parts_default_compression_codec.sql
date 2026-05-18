-- Tags: no-random-settings

-- Test that `system.parts.default_compression_codec` reports the codec
-- selected by the table-level `default_compression_codec` `MergeTree`
-- setting, both after `INSERT` and after `MERGE`, and for projection
-- parts as well.
-- https://github.com/ClickHouse/ClickHouse/issues/84440

DROP TABLE IF EXISTS t_default_codec;

CREATE TABLE t_default_codec
(
    x String,
    PROJECTION p (SELECT x ORDER BY x)
)
ENGINE = MergeTree
ORDER BY x
SETTINGS default_compression_codec = 'ZSTD(3)';

INSERT INTO t_default_codec VALUES ('hello');
INSERT INTO t_default_codec VALUES ('world');

-- After INSERT: the two parts must already use the configured codec.
SELECT 'parts after insert';
SELECT default_compression_codec
FROM system.parts
WHERE database = currentDatabase() AND table = 't_default_codec' AND active
ORDER BY name;

-- Projection parts after INSERT must also use the configured codec.
SELECT 'projection_parts after insert';
SELECT default_compression_codec
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_default_codec' AND active
ORDER BY parent_name, name;

OPTIMIZE TABLE t_default_codec FINAL;

-- After MERGE: the merged part must still use the configured codec.
SELECT 'parts after merge';
SELECT default_compression_codec
FROM system.parts
WHERE database = currentDatabase() AND table = 't_default_codec' AND active;

-- Projection parts after MERGE must also use the configured codec.
SELECT 'projection_parts after merge';
SELECT default_compression_codec
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_default_codec' AND active
ORDER BY parent_name, name;

DROP TABLE t_default_codec;
