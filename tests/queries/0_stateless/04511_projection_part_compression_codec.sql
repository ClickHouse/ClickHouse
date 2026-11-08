-- Tags: no-random-settings

-- A projection part must use the same default compression codec as the parent part it
-- belongs to. Previously the projection writer always resolved the codec with a part size
-- of `0`, so a projection rebuilt for a large or recompressed parent part was left on the
-- flat built-in default (`LZ4`) while the parent switched to a stronger codec.
-- Here the recompression TTL forces the parent part to `NONE` during the merge, and the
-- projection is rebuilt during that same merge (it is not materialized on insert); the
-- rebuilt projection part must inherit the parent's `NONE` codec. Adaptive selection is enabled
-- to ensure it cannot replace the explicit recompression codec with `T64`.

DROP TABLE IF EXISTS t_proj_codec;

CREATE TABLE t_proj_codec
(
    dt DateTime,
    x UInt64,
    PROJECTION p (SELECT x ORDER BY x)
)
ENGINE = MergeTree
ORDER BY tuple()
TTL dt + INTERVAL 1 SECOND RECOMPRESS CODEC(NONE)
SETTINGS
    materialize_projections_on_insert = 0,
    materialize_projections_on_merge = 1,
    enable_adaptive_codec_selection = 1,
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

-- The parent part is recompressed to NONE ...
SELECT 'parent', default_compression_codec
FROM system.parts
WHERE database = currentDatabase() AND table = 't_proj_codec' AND active;

-- ... and the projection rebuilt during the same merge must use the same codec, not LZ4.
SELECT 'projection', name, default_compression_codec
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_codec' AND active;

-- Projection data must really use NONE. If the projection writer re-enabled adaptive selection,
-- monotonic x would be written with T64 and be smaller than its uncompressed representation.
SELECT 'projection data uses NONE', min(data_compressed_bytes >= data_uncompressed_bytes)
FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_proj_codec' AND active AND column = 'x';

DROP TABLE t_proj_codec;
