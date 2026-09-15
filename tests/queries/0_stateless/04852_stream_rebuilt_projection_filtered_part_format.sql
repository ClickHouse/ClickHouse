DROP TABLE IF EXISTS stream_rebuilt_projection_filtered_part_format;

-- A filtered projection can initially have the same row density as its parent,
-- then discard the remaining source rows. Its final part format must use the
-- materialized rows rather than that early density estimate.
CREATE TABLE stream_rebuilt_projection_filtered_part_format
(
    k UInt64,
    v UInt64,
    PROJECTION p_ordered (SELECT k, v WHERE k < 4 ORDER BY k),
    PROJECTION p_reordered (SELECT k, v WHERE k < 4 ORDER BY -k),
    PROJECTION p_ordered_wide (SELECT k, v WHERE k < 21 ORDER BY k),
    PROJECTION p_reordered_wide (SELECT k, v WHERE k < 21 ORDER BY -k)
)
ENGINE = MergeTree
ORDER BY (k, v)
SETTINGS
    enable_vertical_merge_algorithm = 0,
    materialize_projections_on_insert = 0,
    materialize_projections_on_merge = 1,
    merge_max_block_size = 1,
    merge_selector_algorithm = 'Manual',
    min_rows_for_wide_part = 20,
    min_bytes_for_wide_part = 0,
    min_level_for_wide_part = 0;

SYSTEM STOP MERGES stream_rebuilt_projection_filtered_part_format;

-- The small projections keep the first four one-row blocks and filter the
-- other twenty. Before the format-selection delay, the first block extrapolated
-- to all 24 parent rows and irreversibly selected `Wide`. The other filtered
-- projections keep 21 rows to cover the eventual `Wide` decision.
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (0, 0);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (1, 1);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (2, 2);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (3, 3);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (4, 4);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (5, 5);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (6, 6);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (7, 7);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (8, 8);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (9, 9);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (10, 10);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (11, 11);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (12, 12);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (13, 13);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (14, 14);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (15, 15);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (16, 16);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (17, 17);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (18, 18);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (19, 19);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (20, 20);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (21, 21);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (22, 22);
INSERT INTO stream_rebuilt_projection_filtered_part_format VALUES (23, 23);

SELECT count()
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_filtered_part_format' AND active;

SET min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0;
SYSTEM START MERGES stream_rebuilt_projection_filtered_part_format;
OPTIMIZE TABLE stream_rebuilt_projection_filtered_part_format FINAL;

SELECT name, part_type, rows
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_filtered_part_format' AND active
ORDER BY name;
SELECT count(), sum(v)
FROM mergeTreeProjection(currentDatabase(), stream_rebuilt_projection_filtered_part_format, p_ordered);
SELECT count(), sum(v)
FROM mergeTreeProjection(currentDatabase(), stream_rebuilt_projection_filtered_part_format, p_reordered);
SELECT count(), sum(v)
FROM mergeTreeProjection(currentDatabase(), stream_rebuilt_projection_filtered_part_format, p_ordered_wide);
SELECT count(), sum(v)
FROM mergeTreeProjection(currentDatabase(), stream_rebuilt_projection_filtered_part_format, p_reordered_wide);
CHECK TABLE stream_rebuilt_projection_filtered_part_format SETTINGS check_query_single_value_result = 1;

DROP TABLE stream_rebuilt_projection_filtered_part_format;
