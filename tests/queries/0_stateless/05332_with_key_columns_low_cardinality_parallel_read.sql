-- Tags: no-random-merge-tree-settings
SET allow_experimental_map_key_columns = 1;
-- Tag no-random-merge-tree-settings: the randomized `map_serialization_version` replaces `with_key_columns`

-- Several threads reading one part share the cached deserialization prefixes. Each reader must get
-- its own per-key `LowCardinality` state, otherwise the full map is read with wrong values.

DROP TABLE IF EXISTS t_wkc_lc_parallel;

CREATE TABLE t_wkc_lc_parallel (id UInt64, m Map(String, LowCardinality(String)))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    index_granularity = 8192,
    index_granularity_bytes = '10Mi';

INSERT INTO t_wkc_lc_parallel SELECT number, map('a', 'x', 'b', toString(number % 3)) FROM numbers(300000)
SETTINGS max_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

SET max_threads = 8, merge_tree_use_deserialization_prefixes_cache = 1, merge_tree_min_rows_for_concurrent_read = 8192, merge_tree_min_bytes_for_concurrent_read = 0;

SELECT count(), countIf(m = map('a', 'x', 'b', toString(id % 3))) FROM t_wkc_lc_parallel;
SELECT count(), countIf(m = map('a', 'x', 'b', toString(id % 3))) FROM t_wkc_lc_parallel;
SELECT count(), countIf(m = map('a', 'x', 'b', toString(id % 3))) FROM t_wkc_lc_parallel;
SELECT sum(length(toString(m))) FROM t_wkc_lc_parallel;

DROP TABLE t_wkc_lc_parallel;
