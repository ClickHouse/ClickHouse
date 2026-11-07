-- Tags: no-fasttest, no-replicated-database

-- Regression test for the `borrow_from_cache` read wrapper (`HoldingReadBuffer`).
-- The borrowed cache file is read through `ReadBufferFromRemoteFSGather`, which sets a right
-- bound on the per-object buffer (`setReadUntilPosition`) whenever a requested range ends inside
-- the object, and uses `readBigAt` / `supportsReadAt` for positional reads. The wrapper must
-- forward these methods to the inner file buffer; a bare decorator silently drops the bound and
-- disables positional reads, so a sub-range read could over-read past its boundary.
--
-- Force the synchronous (non-async) read path (`remote_filesystem_read_method = 'read'`) so the
-- gather buffer is used directly: there is no `AsynchronousBoundedReadBuffer` above it to trim an
-- over-read, hence an incorrectly-forwarded bound would surface as wrong results here.

SET remote_filesystem_read_method = 'read';
SET local_filesystem_read_method = 'pread';

-- First, create a filesystem cache by making a cached disk.
DROP TABLE IF EXISTS tmp_cache_creator_04489;
CREATE TABLE tmp_cache_creator_04489 (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS disk = disk(
    type = cache,
    disk = 'local_disk',
    name = '04489_cache_creator',
    path = '04489_borrow_test_cache/',
    max_size = '100Mi',
    load_metadata_asynchronously = 0
);

-- Create a table that borrows space from that cache.
DROP TABLE IF EXISTS tmp_borrowed_04489;
CREATE TABLE tmp_borrowed_04489 (key UInt64, value String)
ENGINE = MergeTree() ORDER BY key
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = 'borrow_from_cache',
    metadata_type = 'memory',
    cache_name = '04489_cache_creator',
    name = '04489_borrowed_disk'
), index_granularity = 8192;

-- Enough rows that each column spans many granules inside a single part, so a bounded sub-range
-- read ends strictly inside a borrowed object rather than at its natural end of file.
INSERT INTO tmp_borrowed_04489 SELECT number, toString(number) FROM numbers(100000);

-- Bounded sub-range reads: the read until position ends inside the object.
SELECT count(), sum(key) FROM tmp_borrowed_04489 WHERE key < 1000;
SELECT key, value FROM tmp_borrowed_04489 ORDER BY key LIMIT 5;
SELECT key, value FROM tmp_borrowed_04489 WHERE key IN (0, 12345, 99999) ORDER BY key;

-- Full read for good measure.
SELECT count(), sum(key), sum(length(value)) FROM tmp_borrowed_04489;

DROP TABLE tmp_borrowed_04489;
DROP TABLE tmp_cache_creator_04489;
