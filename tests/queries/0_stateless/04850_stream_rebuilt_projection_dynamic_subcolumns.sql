DROP TABLE IF EXISTS stream_rebuilt_projection_dynamic_subcolumns;

CREATE TABLE stream_rebuilt_projection_dynamic_subcolumns
(
    k UInt64,
    raw String,
    PROJECTION p_direct
    (
        SELECT k, CAST(concat(raw, ''), 'JSON(max_dynamic_paths=4)') AS json
        WHERE k >= 0
        ORDER BY k
    ) WITH SETTINGS (min_bytes_for_wide_part = 1000000000),
    PROJECTION p_reordered
    (
        SELECT k, CAST(concat(raw, ''), 'JSON(max_dynamic_paths=4)') AS json
        WHERE k >= 0
        ORDER BY -k
    ) WITH SETTINGS (min_bytes_for_wide_part = 1000000000)
)
ENGINE = MergeTree
ORDER BY k
SETTINGS
    enable_vertical_merge_algorithm = 0,
    materialize_projections_on_insert = 0,
    materialize_projections_on_merge = 1,
    merge_max_block_size = 1,
    merge_selector_algorithm = 'Manual',
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0,
    min_level_for_wide_part = 0,
    merge_max_dynamic_subcolumns_in_wide_part = 2,
    merge_max_dynamic_subcolumns_in_compact_part = 10;

SYSTEM STOP MERGES stream_rebuilt_projection_dynamic_subcolumns;

-- The parent parts are `Wide` while both projections are `Compact`. Twelve one-row
-- blocks also force the reordered projection through bounded external-sort merging.
INSERT INTO stream_rebuilt_projection_dynamic_subcolumns VALUES (0, '{"a" : 42, "b" : 42, "c" : 42, "d" : 42, "e" : 42, "f" : 42}');
INSERT INTO stream_rebuilt_projection_dynamic_subcolumns VALUES (1, '{"a" : 42, "b" : 42, "c" : 42, "d" : 42, "e" : 42, "f" : 42}');
INSERT INTO stream_rebuilt_projection_dynamic_subcolumns VALUES (2, '{"a" : 42, "b" : 42, "c" : 42, "d" : 42, "e" : 42, "f" : 42}');
INSERT INTO stream_rebuilt_projection_dynamic_subcolumns VALUES (3, '{"a" : 42, "b" : 42, "c" : 42, "d" : 42, "e" : 42, "f" : 42}');
INSERT INTO stream_rebuilt_projection_dynamic_subcolumns VALUES (4, '{"a" : 42, "b" : 42, "c" : 42, "d" : 42, "e" : 42, "f" : 42}');
INSERT INTO stream_rebuilt_projection_dynamic_subcolumns VALUES (5, '{"a" : 42, "b" : 42, "c" : 42, "d" : 42, "e" : 42, "f" : 42}');
INSERT INTO stream_rebuilt_projection_dynamic_subcolumns VALUES (6, '{"a" : 42, "b" : 42, "c" : 42, "d" : 42, "e" : 42, "f" : 42}');
INSERT INTO stream_rebuilt_projection_dynamic_subcolumns VALUES (7, '{"a" : 42, "b" : 42, "c" : 42, "d" : 42, "e" : 42, "f" : 42}');
INSERT INTO stream_rebuilt_projection_dynamic_subcolumns VALUES (8, '{"a" : 42, "b" : 42, "c" : 42, "d" : 42, "e" : 42, "f" : 42}');
INSERT INTO stream_rebuilt_projection_dynamic_subcolumns VALUES (9, '{"a" : 42, "b" : 42, "c" : 42, "d" : 42, "e" : 42, "f" : 42}');
INSERT INTO stream_rebuilt_projection_dynamic_subcolumns VALUES (10, '{"a" : 42, "b" : 42, "c" : 42, "d" : 42, "e" : 42, "f" : 42}');
INSERT INTO stream_rebuilt_projection_dynamic_subcolumns VALUES (11, '{"a" : 42, "b" : 42, "c" : 42, "d" : 42, "e" : 42, "f" : 42}');

SELECT count()
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_dynamic_subcolumns' AND active;

SET min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0;
SYSTEM START MERGES stream_rebuilt_projection_dynamic_subcolumns;
OPTIMIZE TABLE stream_rebuilt_projection_dynamic_subcolumns FINAL;

SELECT name, part_type
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_dynamic_subcolumns' AND active
ORDER BY name;
-- Projection aliases are not retained in the exposed schema. Inspect the
-- materialized `JSON` expression and its persisted `Dynamic` and shared paths.
SELECT
    JSONDynamicPaths(`CAST(concat(raw, ''), 'JSON(max_dynamic_paths=4)')`),
    JSONSharedDataPaths(`CAST(concat(raw, ''), 'JSON(max_dynamic_paths=4)')`)
FROM mergeTreeProjection(currentDatabase(), stream_rebuilt_projection_dynamic_subcolumns, p_direct)
LIMIT 1;
SELECT
    JSONDynamicPaths(`CAST(concat(raw, ''), 'JSON(max_dynamic_paths=4)')`),
    JSONSharedDataPaths(`CAST(concat(raw, ''), 'JSON(max_dynamic_paths=4)')`)
FROM mergeTreeProjection(currentDatabase(), stream_rebuilt_projection_dynamic_subcolumns, p_reordered)
LIMIT 1;
SELECT count()
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_dynamic_subcolumns' AND active;
CHECK TABLE stream_rebuilt_projection_dynamic_subcolumns SETTINGS check_query_single_value_result = 1;

DROP TABLE stream_rebuilt_projection_dynamic_subcolumns;
