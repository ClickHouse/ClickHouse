DROP TABLE IF EXISTS stream_rebuilt_projection_blocks;

CREATE TABLE stream_rebuilt_projection_blocks
(
    k UInt64,
    v UInt64,
    extra UInt64,
    PROJECTION p_normal (SELECT k, v ORDER BY k),
    PROJECTION p_aggregate (SELECT k, sum(v) AS sum_v GROUP BY k),
    PROJECTION p_reordered (SELECT k, v ORDER BY v),
    PROJECTION p_reordered_aggregate (SELECT v % 5 AS g, sum(v) AS sum_v GROUP BY g)
)
ENGINE = MergeTree
ORDER BY (k, v)
SETTINGS
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    materialize_projections_on_insert = 0,
    materialize_projections_on_merge = 1,
    merge_max_block_size = 1,
    merge_selector_algorithm = 'Manual',
    min_rows_for_wide_part = 1,
    min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES stream_rebuilt_projection_blocks;

-- Keep the source projections absent, then rebuild from more blocks than the
-- former temporary projection merge fan-in. The unused `extra` column forces the
-- vertical merge path, while the small blocks split projection aggregate groups.
INSERT INTO stream_rebuilt_projection_blocks SELECT number % 3, number, number FROM numbers(4);
INSERT INTO stream_rebuilt_projection_blocks SELECT number % 3, number + 4, number + 4 FROM numbers(4);
INSERT INTO stream_rebuilt_projection_blocks SELECT number % 3, number + 8, number + 8 FROM numbers(4);
INSERT INTO stream_rebuilt_projection_blocks SELECT number % 3, number + 12, number + 12 FROM numbers(4);
INSERT INTO stream_rebuilt_projection_blocks SELECT number % 3, number + 16, number + 16 FROM numbers(4);
INSERT INTO stream_rebuilt_projection_blocks SELECT number % 3, number + 20, number + 20 FROM numbers(4);
INSERT INTO stream_rebuilt_projection_blocks SELECT number % 3, number + 24, number + 24 FROM numbers(4);
INSERT INTO stream_rebuilt_projection_blocks SELECT number % 3, number + 28, number + 28 FROM numbers(4);
INSERT INTO stream_rebuilt_projection_blocks SELECT number % 3, number + 32, number + 32 FROM numbers(4);
INSERT INTO stream_rebuilt_projection_blocks SELECT number % 3, number + 36, number + 36 FROM numbers(4);
INSERT INTO stream_rebuilt_projection_blocks SELECT number % 3, number + 40, number + 40 FROM numbers(4);
INSERT INTO stream_rebuilt_projection_blocks SELECT number % 3, number + 44, number + 44 FROM numbers(4);

SET min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0;
SYSTEM START MERGES stream_rebuilt_projection_blocks;
OPTIMIZE TABLE stream_rebuilt_projection_blocks FINAL;

SELECT count(), sum(v), sum(k) FROM mergeTreeProjection(currentDatabase(), stream_rebuilt_projection_blocks, p_normal);
SELECT k, sumMerge(`sum(v)`) FROM mergeTreeProjection(currentDatabase(), stream_rebuilt_projection_blocks, p_aggregate) GROUP BY k ORDER BY k;
SELECT count(), sum(v), sum(k) FROM mergeTreeProjection(currentDatabase(), stream_rebuilt_projection_blocks, p_reordered);
SELECT `modulo(v, 5)`, sumMerge(`sum(v)`) FROM mergeTreeProjection(currentDatabase(), stream_rebuilt_projection_blocks, p_reordered_aggregate) GROUP BY `modulo(v, 5)` ORDER BY `modulo(v, 5)`;
SELECT sum(extra) FROM stream_rebuilt_projection_blocks;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_blocks' AND active;
SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_blocks' AND active;
-- All rebuilt projections must be attached to the single final parent part.
SELECT countDistinct(parent_name) FROM system.projection_parts WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_blocks' AND active;
SELECT countDistinct(parent_uuid) FROM system.projection_parts WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_blocks' AND active;
CHECK TABLE stream_rebuilt_projection_blocks SETTINGS check_query_single_value_result = 1;

DROP TABLE stream_rebuilt_projection_blocks;
