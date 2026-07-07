-- Tags: no-random-settings

-- A projection part must use the same default compression codec as the parent part it
-- belongs to. Previously the projection writer always resolved the codec with a part size
-- of `0`, so a projection rebuilt for a large or recompressed parent part was left on the
-- flat built-in default (`LZ4`) while the parent switched to a stronger codec.
-- Here the recompression TTL forces the parent part to `ZSTD(1)` during the merge, and the
-- projection is rebuilt during that same merge (it is not materialized on insert); the
-- rebuilt projection part must inherit the parent's `ZSTD(1)` codec.

DROP TABLE IF EXISTS t_proj_codec;

CREATE TABLE t_proj_codec
(
    dt DateTime,
    x UInt64,
    PROJECTION p (SELECT x ORDER BY x)
)
ENGINE = MergeTree
ORDER BY tuple()
TTL dt + INTERVAL 1 SECOND RECOMPRESS CODEC(ZSTD(1))
SETTINGS
    materialize_projections_on_insert = 0,
    materialize_projections_on_merge = 1,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

SYSTEM STOP TTL MERGES t_proj_codec;

-- Insert with an already-expired recompression TTL. The projection is not written yet.
INSERT INTO t_proj_codec SELECT now() - INTERVAL 1 DAY, number FROM numbers(1000);

SELECT 'projection parts before merge', count()
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_codec' AND active;

SYSTEM START TTL MERGES t_proj_codec;
OPTIMIZE TABLE t_proj_codec FINAL;

-- The parent part is recompressed to ZSTD(1) ...
SELECT 'parent', default_compression_codec
FROM system.parts
WHERE database = currentDatabase() AND table = 't_proj_codec' AND active;

-- ... and the projection rebuilt during the same merge must use the same codec, not LZ4.
SELECT 'projection', name, default_compression_codec
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_codec' AND active;

DROP TABLE t_proj_codec;
