DROP TABLE IF EXISTS stream_rebuilt_projection_index;

CREATE TABLE stream_rebuilt_projection_index
(
    id UInt64,
    region UInt8,
    PROJECTION region_idx INDEX region TYPE basic
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    enable_vertical_merge_algorithm = 0,
    materialize_projections_on_insert = 0,
    materialize_projections_on_merge = 1,
    merge_max_block_size = 1,
    merge_selector_algorithm = 'Manual',
    index_granularity = 1;

SYSTEM STOP MERGES stream_rebuilt_projection_index;

-- Keep the source index projections absent. Rebuilding the index reorders the
-- parent stream by region while preserving the parent-row offsets.
INSERT INTO stream_rebuilt_projection_index SELECT number, number % 3 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_index SELECT number + 3, number % 3 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_index SELECT number + 6, number % 3 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_index SELECT number + 9, number % 3 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_index SELECT number + 12, number % 3 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_index SELECT number + 15, number % 3 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_index SELECT number + 18, number % 3 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_index SELECT number + 21, number % 3 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_index SELECT number + 24, number % 3 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_index SELECT number + 27, number % 3 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_index SELECT number + 30, number % 3 FROM numbers(3);
INSERT INTO stream_rebuilt_projection_index SELECT number + 33, number % 3 FROM numbers(3);

SELECT count()
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_index' AND active;

SET min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0;
SYSTEM START MERGES stream_rebuilt_projection_index;
OPTIMIZE TABLE stream_rebuilt_projection_index FINAL;

SET optimize_use_projections = 1, optimize_use_projection_filtering = 1, min_table_rows_to_use_projection_index = 0;

SELECT count(), sum(id), sum(region) FROM stream_rebuilt_projection_index;
SELECT id FROM stream_rebuilt_projection_index WHERE region = 1 ORDER BY id;
SELECT count() FROM mergeTreeProjection(currentDatabase(), stream_rebuilt_projection_index, region_idx);
SELECT count()
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 'stream_rebuilt_projection_index' AND active;
CHECK TABLE stream_rebuilt_projection_index SETTINGS check_query_single_value_result = 1;

DROP TABLE stream_rebuilt_projection_index;
