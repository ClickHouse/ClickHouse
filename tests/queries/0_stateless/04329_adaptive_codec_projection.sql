-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: reads system.parts_columns, which randomised enable_block_number/offset_column add rows to.

DROP TABLE IF EXISTS t_adaptive_projection;
DROP TABLE IF EXISTS t_adaptive_projection_off;

CREATE TABLE t_adaptive_projection
(
    k UInt64,
    v UInt64,
    PROJECTION p_order (SELECT v, k ORDER BY v)
)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, allow_experimental_adaptive_codec_selection = 1;

CREATE TABLE t_adaptive_projection_off AS t_adaptive_projection
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, allow_experimental_adaptive_codec_selection = 0;

INSERT INTO t_adaptive_projection SELECT number, 99999 - number FROM numbers(50000);
INSERT INTO t_adaptive_projection SELECT number, 99999 - number FROM numbers(50000, 50000);
OPTIMIZE TABLE t_adaptive_projection FINAL;

INSERT INTO t_adaptive_projection_off SELECT number, 99999 - number FROM numbers(50000);
INSERT INTO t_adaptive_projection_off SELECT number, 99999 - number FROM numbers(50000, 50000);
OPTIMIZE TABLE t_adaptive_projection_off FINAL;

-- The parent part went adaptive (T64).
SELECT column, mapContains(codec_block_counts, 'T64') AS has_t64
FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_adaptive_projection' AND active ORDER BY column;

-- The projection went adaptive: its columns compress smaller than the same data with the default codec.
SELECT
    (SELECT sum(data_compressed_bytes) FROM system.projection_parts_columns
     WHERE database = currentDatabase() AND table = 't_adaptive_projection' AND active)
  < (SELECT sum(data_compressed_bytes) FROM system.projection_parts_columns
     WHERE database = currentDatabase() AND table = 't_adaptive_projection_off' AND active) AS projection_smaller;

-- The merged part carries the projection with all rows.
SELECT name, rows FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_adaptive_projection' AND active;

-- Read the projection.
SELECT k FROM t_adaptive_projection WHERE v = 12345 SETTINGS force_optimize_projection = 1;
SELECT count(), sum(v) FROM t_adaptive_projection WHERE v < 1000 SETTINGS force_optimize_projection = 1;

-- Read the parent.
SELECT count(), sum(k), sum(v) FROM t_adaptive_projection SETTINGS optimize_use_projections = 0;

CHECK TABLE t_adaptive_projection SETTINGS check_query_single_value_result = 1;

DROP TABLE t_adaptive_projection;
DROP TABLE t_adaptive_projection_off;
