DROP TABLE IF EXISTS 03657_gby_overflow;

CREATE TABLE 03657_gby_overflow(key UInt64, val UInt16) ENGINE = MergeTree ORDER BY tuple()
AS SELECT number, 0 from numbers(100000);

SELECT key, any(val) FROM 03657_gby_overflow GROUP BY key ORDER BY key LIMIT 10
SETTINGS group_by_overflow_mode = 'any',
         max_rows_to_group_by = 100,
         max_threads = 1,
         max_block_size = 100,
         group_by_two_level_threshold = 1000000000,
         group_by_two_level_threshold_bytes = 1000000000;

DROP TABLE 03657_gby_overflow;
