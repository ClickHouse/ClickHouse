-- Regression test for in-place modification of the string column kept in the deserialization
-- state of the `.size` subcolumn (`single_stream` format) while it is still shared with the
-- emitted full column through the substreams cache. The discontiguous key ranges make one block
-- span several `readRows` calls, so later ranges append to the shared column. The corruption is
-- only observable as UAF under ASan; the assertions below pin the correct results.

DROP TABLE IF EXISTS t_string_size_single_stream;

CREATE TABLE t_string_size_single_stream (k UInt64, s String)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 32,
         min_bytes_for_wide_part = 0,
         serialization_info_version = 'with_types',
         string_serialization_version = 'single_stream';

INSERT INTO t_string_size_single_stream SELECT number, repeat('a', number % 80) FROM numbers(20000);

OPTIMIZE TABLE t_string_size_single_stream FINAL;

-- `.size` before the full column: the state column filled by the `.size` read is the shared one.
SELECT sum(s.size), countIf(s = ''), countIf(s.size != length(s)), count()
FROM t_string_size_single_stream
WHERE (k >= 0 AND k < 320) OR (k >= 640 AND k < 960) OR (k >= 1280 AND k < 1600)
   OR (k >= 1920 AND k < 2240) OR (k >= 2560 AND k < 2880) OR (k >= 3200 AND k < 3520)
SETTINGS max_threads = 1, max_block_size = 65536, optimize_functions_to_subcolumns = 0;

-- Opposite column order.
SELECT countIf(s = ''), sum(s.size), countIf(s.size != length(s)), count()
FROM t_string_size_single_stream
WHERE (k >= 0 AND k < 320) OR (k >= 640 AND k < 960) OR (k >= 1280 AND k < 1600)
   OR (k >= 1920 AND k < 2240) OR (k >= 2560 AND k < 2880) OR (k >= 3200 AND k < 3520)
SETTINGS max_threads = 1, max_block_size = 65536, optimize_functions_to_subcolumns = 0;

-- Control: only `.size`, no sharing with the full column. Must produce the same sum and count.
SELECT sum(s.size), count()
FROM t_string_size_single_stream
WHERE (k >= 0 AND k < 320) OR (k >= 640 AND k < 960) OR (k >= 1280 AND k < 1600)
   OR (k >= 1920 AND k < 2240) OR (k >= 2560 AND k < 2880) OR (k >= 3200 AND k < 3520)
SETTINGS max_threads = 1, max_block_size = 65536, optimize_functions_to_subcolumns = 0;

DROP TABLE t_string_size_single_stream;
