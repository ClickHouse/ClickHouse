-- Tags: no-fasttest, no-replicated-database

-- Regression test: an empty append on a `borrow_from_cache` / `memory` disk must not record a
-- phantom blob. `StripeLogSink` opens an append buffer before any rows are written and always
-- finalizes it, so `INSERT ... SELECT ... LIMIT 0` used to leave metadata pointing at a cache
-- segment that was never created, and a later read failed with `FILE_DOESNT_EXIST`.

-- First, create a filesystem cache by making a cached disk.
DROP TABLE IF EXISTS tmp_cache_creator;
CREATE TABLE tmp_cache_creator (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS disk = disk(
    type = cache,
    disk = 'local_disk',
    name = '04727_cache_creator',
    path = '04727_borrow_test_cache/',
    max_size = '100Mi',
    load_metadata_asynchronously = 0
);

-- Register a named borrow disk. The log family accepts only a named disk (not an inline
-- definition), so the disk is introduced via a throwaway MergeTree table.
DROP TABLE IF EXISTS tmp_disk_creator;
CREATE TABLE tmp_disk_creator (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = 'borrow_from_cache',
    metadata_type = 'memory',
    cache_name = '04727_cache_creator',
    name = '04727_borrowed_disk'
);

DROP TABLE IF EXISTS tmp_stripe_log;
CREATE TABLE tmp_stripe_log (key UInt64, value String)
ENGINE = StripeLog
SETTINGS disk = '04727_borrowed_disk';

-- Empty append into a fresh table: the data files are created but must reference no blob.
INSERT INTO tmp_stripe_log SELECT number, toString(number) FROM numbers(10) LIMIT 0;
SELECT count() FROM tmp_stripe_log;

-- Real data still works after the empty append.
INSERT INTO tmp_stripe_log VALUES (1, 'hello'), (2, 'world');
SELECT * FROM tmp_stripe_log ORDER BY key;

-- Empty append after real data: reading back must not hit a phantom blob at the end of the file.
INSERT INTO tmp_stripe_log SELECT number, toString(number) FROM numbers(10) LIMIT 0;
SELECT * FROM tmp_stripe_log ORDER BY key;

INSERT INTO tmp_stripe_log VALUES (3, 'again');
SELECT count() FROM tmp_stripe_log;

-- Clean up
DROP TABLE tmp_stripe_log;
DROP TABLE tmp_disk_creator;
DROP TABLE tmp_cache_creator;
