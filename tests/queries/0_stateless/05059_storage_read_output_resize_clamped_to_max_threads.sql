-- Tags: no-fasttest

-- A post-read resize must not widen a storage read's output beyond the thread budget.
-- `max_memory_usage` keeps a regression a loud query error instead of an OOM-killed server.
-- `max_threads_min_free_memory_per_thread = 0` pins the adaptive thread cap so the widths
-- asserted below do not drop under memory pressure.

SET parallelize_output_from_storages = 1;
SET max_memory_usage = '2G';

DROP TABLE IF EXISTS t_resize_file;
DROP TABLE IF EXISTS t_resize_join;
DROP TABLE IF EXISTS t_resize_object_storage;

-- Carrier: ReadFromFile::initializePipeline
CREATE TABLE t_resize_file (n UInt64) ENGINE = File(TabSeparated);
INSERT INTO t_resize_file VALUES (1), (2), (3);

SELECT sum(n) FROM t_resize_file
SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 1000000,
         max_threads_min_free_memory_per_thread = 0;

-- Carrier: IStorage::read (Join discards num_streams and returns a single source)
CREATE TABLE t_resize_join (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO t_resize_join VALUES (1, 10), (2, 20);

SELECT sum(v) FROM t_resize_join
SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 1000000,
         max_threads_min_free_memory_per_thread = 0;

-- Carrier: IStorage::read, the shape that reddened `Stress test (azure, amd_tsan)` on master.
-- `GenerateRandom` lowers its own source count to what the trivial `LIMIT` needs, so the count
-- that reaches its guard against an absurd request is already small; the post-read resize then
-- widens the output back to the whole ratio-expanded request.
SELECT count() FROM (SELECT n FROM generateRandom('n UInt8') LIMIT 1)
SETTINGS optimize_trivial_count_query = 0, max_block_size = 1, preferred_block_size_bytes = 0,
         max_threads = 4, max_streams_to_max_threads_ratio = 1000000,
         max_threads_min_free_memory_per_thread = 0;

-- A bare SELECT * leaves the storage read as the only resize producer, and the width must
-- still reach max_threads even though the Join source ignores num_streams.
SELECT match(arrayStringConcat(groupArray(explain), ''), '.*Resize 1 → 2 *Join 0 → 1 *$')
FROM (
    EXPLAIN PIPELINE SELECT * FROM t_resize_join
    SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 1000000,
             max_threads_min_free_memory_per_thread = 0
);

-- The resize itself must survive: the output is still widened to max_threads, so a fix that
-- clamped all the way down to the source count would fail here. A bare SELECT * has exactly
-- one resize producer, the storage read, so the match is not satisfiable by any other step.
SELECT match(arrayStringConcat(groupArray(explain), ''), '.*Resize 1 → 4 *File 0 → 1 *$')
FROM (
    EXPLAIN PIPELINE SELECT * FROM t_resize_file
    SETTINGS max_threads = 4, max_streams_to_max_threads_ratio = 1000000,
             max_threads_min_free_memory_per_thread = 0
);

-- Carrier: ReadFromURL::initializePipeline. EXPLAIN only, so the host is never contacted.
SELECT match(arrayStringConcat(groupArray(explain), ''), '.*Resize 1 → 2 *URL 0 → 1 *$')
FROM (
    EXPLAIN PIPELINE SELECT * FROM url('https://example.com', Parquet, 'x Int64')
    SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 1000000,
             max_threads_min_free_memory_per_thread = 0
);

-- Carrier: ReadFromObjectStorageStep::initializePipeline. EXPLAIN only, so no endpoint is
-- contacted; a single key keeps num_streams at 1 so the resize guard is reached.
CREATE TABLE t_resize_object_storage (x UInt64, y String)
ENGINE = S3('http://localhost:19999/dummy.parquet', NOSIGN, Parquet);

SELECT match(arrayStringConcat(groupArray(explain), ''), '.*Resize 1 → 2 *ReadFromObjectStorage 0 → 1 *$')
FROM (
    EXPLAIN PIPELINE SELECT * FROM t_resize_object_storage
    SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 1000000,
             max_threads_min_free_memory_per_thread = 0
);

DROP TABLE t_resize_file;
DROP TABLE t_resize_join;
DROP TABLE t_resize_object_storage;
