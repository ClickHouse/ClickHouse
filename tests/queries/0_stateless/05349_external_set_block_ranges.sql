-- The set holds the multiples of 3 * 2^32 by the numbers below 20480. The gaps between them are too wide to
-- pack, so each block but the last holds 512 keys as they are, and block `b` holds the multiples by the
-- numbers from `512 * b` up to `512 * b + 511`, in 40 blocks. The frame of such a block takes 4126 bytes: a
-- checksum of 16 bytes, the header of the codec of 9 bytes and the block of 4101 bytes. Each query probes the
-- first key of some blocks, and the lookups of a chunk read the frames of the blocks that it probes in
-- ranges. A range takes at most 128 KiB, which holds 31 such frames, and it joins frames that at most 32 KiB
-- separate, which is 7 such frames. With a threshold of 1 byte, each set spills before its first chunk, and
-- a cache of 128 MiB keeps every block that the lookups read.
SET max_threads = 1;
SET max_block_size = 65536;
SET max_bytes_before_external_set = 1;
SET max_bytes_for_external_set_cache = 134217728;

-- The 40 blocks fill a range of 31 frames and a range of 9 frames, which ends at the end of the file.
SELECT 'consecutive', count() FROM numbers(40)
WHERE number * 512 * 12884901888 IN (SELECT number * 12884901888 FROM numbers(20480))
SETTINGS log_comment = '05349_external_set_block_ranges/consecutive';

-- 7 frames between the probed blocks do not separate them, so the first 4 blocks share a range of 25 frames.
-- The fifth block does not fit in that range and takes a range of its own.
SELECT 'every eighth', count() FROM numbers(5)
WHERE number * 8 * 512 * 12884901888 IN (SELECT number * 12884901888 FROM numbers(20480))
SETTINGS log_comment = '05349_external_set_block_ranges/every eighth';

-- 8 frames between the probed blocks separate them, so each block takes a range of its own.
SELECT 'every ninth', count() FROM numbers(5)
WHERE number * 9 * 512 * 12884901888 IN (SELECT number * 12884901888 FROM numbers(20480))
SETTINGS log_comment = '05349_external_set_block_ranges/every ninth';

-- The first chunk of 20 probes reads the even blocks, which the cache keeps. Each of the next two chunks
-- probes 20 consecutive blocks. It takes the even blocks from the cache and reads the odd blocks in a range
-- that also holds the frames of the cached blocks between them.
SELECT 'cached', count() FROM numbers(60)
WHERE if(number < 20, number * 2, number - 20) * 512 * 12884901888 IN (SELECT number * 12884901888 FROM numbers(20480))
SETTINGS max_block_size = 20, log_comment = '05349_external_set_block_ranges/cached';

-- A buffer of temporary files smaller than a block still writes each block as one frame. The first chunk of
-- 40 probes reads every block, and the second chunk takes all of them from the cache.
SELECT 'small buffer', count() FROM numbers(80)
WHERE (number % 40) * 512 * 12884901888 IN (SELECT number * 12884901888 FROM numbers(20480))
SETTINGS max_block_size = 40, temporary_files_buffer_size = 1000, log_comment = '05349_external_set_block_ranges/small buffer';

-- Every set spilled, and the lookups read each probed block once or took it from the cache. The test runner
-- gives the other queries a log comment of its own, so the cases are found by the name of the test that
-- starts theirs.
SYSTEM FLUSH LOGS query_log;
SELECT substring(log_comment, length('05349_external_set_block_ranges/') + 1), ProfileEvents['SetsSpilledToDisk'],
    ProfileEvents['ExternalSetReadBlocks'], ProfileEvents['ExternalSetCacheHits']
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
    AND startsWith(log_comment, '05349_external_set_block_ranges/')
ORDER BY event_time_microseconds;
