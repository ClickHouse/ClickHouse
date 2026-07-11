-- Tags: stateful, no-random-settings, no-parallel
-- no-parallel: Heavy query

SET max_memory_usage = '10G';

SELECT sum(cityHash64(*)) FROM test.hits SETTINGS max_threads=40;

-- We had a bug which lead to additional compressed data read. test.hits compressed size is about 1.2Gb, but we read more then 3Gb.
-- Small additional reads still possible, so the threshold leaves headroom over the
-- measured value. test.hits is served from the baked stateful dataset store - an
-- object-storage-typed disk backed by local files - whose remote read path re-reads
-- the compressed block containing a mark on each seek instead of reusing the buffer
-- like the local read path does, so the scan reads ~1.86Gb from the filesystem for
-- ~1.27Gb of compressed data.
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] < 2500000000 from system.query_log where event_date >= yesterday() AND event_time >= now() - 600 AND query = 'SELECT sum(cityHash64(*)) FROM test.hits SETTINGS max_threads=40;' and current_database = currentDatabase() and type = 'QueryFinish';
