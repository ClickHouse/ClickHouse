-- Tags: no-random-merge-tree-settings
SET allow_experimental_map_key_columns = 1;
-- Tag no-random-merge-tree-settings: the randomized `map_serialization_version` replaces `with_key_columns`

-- The columns cache decides from the streams of a part whether a column has any data in it. The
-- streams of a `with_key_columns` Map are named after the keys of the part, so the full map and
-- the subcolumns derived from all keys must be read from the part, not filled with defaults.

DROP TABLE IF EXISTS t_wkc_columns_cache;

CREATE TABLE t_wkc_columns_cache (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 1000;

INSERT INTO t_wkc_columns_cache SELECT number, if(number % 3 = 0, map('a', number), map('a', number, 'b', number * 2)) FROM numbers(5000);

SET use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1;

-- The table is new, so the first read fills the cache and the second one is served from it.
SELECT sum(length(m)), sum(m['a']), sum(m['b']), sum(length(m.keys)), sum(arraySum(m.values)), sum(m.size0) FROM t_wkc_columns_cache;
SELECT sum(length(m)), sum(m['a']), sum(m['b']), sum(length(m.keys)), sum(arraySum(m.values)), sum(m.size0) FROM t_wkc_columns_cache;
SELECT m FROM t_wkc_columns_cache WHERE id IN (2999, 3000) ORDER BY id;

DROP TABLE t_wkc_columns_cache;
