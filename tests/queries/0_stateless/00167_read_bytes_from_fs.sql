-- Tags: stateful, no-random-settings, no-parallel, no-object-storage
-- no-parallel: Heavy query

SET max_memory_usage = '10G';

SELECT sum(cityHash64(*)) FROM test.hits SETTINGS max_threads=40, log_comment='00167_read_bytes_from_fs';

-- We had a bug which led to reading additional compressed data: test.hits compressed
-- size is about 1.2Gb, but we read more than 3Gb. Compare the counters with the
-- actual on-disk size of the table instead of an absolute threshold, so the test does
-- not depend on the exact dataset or on the storage stack serving it.
--
-- ReadCompressedBytes counts the compressed bytes consumed by the reader and must
-- match the table size almost exactly on any storage - this is the tight guard for
-- the original bug.
--
-- ReadBufferFromFileDescriptorReadBytes counts actual filesystem reads. The baked
-- stateful dataset store is an object-storage-typed disk backed by local files, and
-- its remote read path re-reads the compressed block containing a mark on each seek
-- instead of reusing the buffer like the local read path does. The re-read amount is
-- timing-dependent (measured 1.5x-2x the table size depending on the environment),
-- so 4x is a loose gross-thrash guard only.
SYSTEM FLUSH LOGS query_log;

WITH (SELECT sum(bytes_on_disk) FROM system.parts WHERE database = 'test' AND table = 'hits' AND active) AS table_bytes_on_disk
SELECT
    ProfileEvents['ReadCompressedBytes'] < 1.3 * table_bytes_on_disk,
    ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] < 4 * table_bytes_on_disk
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND log_comment = '00167_read_bytes_from_fs'
    AND current_database = currentDatabase()
    AND type = 'QueryFinish';
