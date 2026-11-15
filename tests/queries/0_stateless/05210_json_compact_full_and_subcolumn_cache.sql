DROP TABLE IF EXISTS json_compact_parent_cache;
CREATE TABLE json_compact_parent_cache
(
    id UInt64,
    j JSON(p UInt64, q String, max_dynamic_paths = 0),
    wrapped Tuple(data JSON(p UInt64, max_dynamic_paths = 0))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '200G', min_rows_for_wide_part = 1,
    write_marks_for_substreams_in_compact_parts = 0, index_granularity = 2;
INSERT INTO json_compact_parent_cache
SELECT number, toJSONString(map('p', toString(number), 'q', toString(number + 1))),
    tuple(toJSONString(map('p', number))) FROM numbers(9);

-- Exercise both request orders, several granules per block, and block boundaries.
SELECT id, j, j.p, j.q, wrapped, wrapped.data.p
FROM json_compact_parent_cache ORDER BY id SETTINGS max_block_size = 3, max_threads = 1;
SELECT j.p, j, wrapped.data.p, wrapped, id
FROM json_compact_parent_cache ORDER BY id SETTINGS max_block_size = 20, max_threads = 1;
DROP TABLE json_compact_parent_cache;
