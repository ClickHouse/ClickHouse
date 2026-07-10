-- Tags: stateful, no-random-settings, no-parallel
-- no-parallel: Heavy query

SET max_memory_usage = '10G';
-- test.hits is stored on an object-storage-typed disk backed by local files (the baked
-- stateful dataset store), which takes the remote read path: it splits reads into
-- merge_tree_min_bytes_per_task_for_remote_reading-sized tasks (2 MB by default) and each
-- task over-reads at its boundary, adding ~25-30% to the bytes read from the filesystem.
-- Use large tasks so the measurement below reflects the actual compressed data read.
SELECT sum(cityHash64(*)) FROM test.hits SETTINGS max_threads=40, merge_tree_min_bytes_per_task_for_remote_reading=268435456;

-- We had a bug which lead to additional compressed data read. test.hits compressed size is about 1.2Gb, but we read more then 3Gb.
-- Small additional reads still possible, so we compare with about 1.5Gb.
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] < 1500000000 from system.query_log where event_date >= yesterday() AND event_time >= now() - 600 AND query = 'SELECT sum(cityHash64(*)) FROM test.hits SETTINGS max_threads=40, merge_tree_min_bytes_per_task_for_remote_reading=268435456;' and current_database = currentDatabase() and type = 'QueryFinish';
