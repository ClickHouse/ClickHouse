CREATE TABLE json_compact_shared_path_cache (id UInt64, j JSON(max_dynamic_paths = 0))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2,
    min_bytes_for_wide_part = '1G', write_marks_for_substreams_in_compact_parts = 0;

INSERT INTO json_compact_shared_path_cache
SELECT number, toJSONString(map('a', number, 'b', number + 1)) FROM numbers(9);

SELECT DISTINCT part_type FROM system.parts
WHERE active AND database = currentDatabase() AND table = 'json_compact_shared_path_cache';

-- Read the full object and two shared paths across multiple granules in each output block.
SELECT count(), countIf(j.a::UInt64 != id OR j.b::UInt64 != id + 1
    OR JSONExtractUInt(toJSONString(j), 'a') != id)
FROM json_compact_shared_path_cache SETTINGS max_block_size = 3, max_threads = 1;
SELECT count(), countIf(j.a::UInt64 != id OR j.b::UInt64 != id + 1
    OR JSONExtractUInt(toJSONString(j), 'a') != id)
FROM json_compact_shared_path_cache SETTINGS max_block_size = 20, max_threads = 1;

DROP TABLE json_compact_shared_path_cache;
