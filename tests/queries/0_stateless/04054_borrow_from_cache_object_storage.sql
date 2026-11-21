-- Tags: no-fasttest, no-replicated-database

-- First, create a filesystem cache by making a cached disk.
DROP TABLE IF EXISTS tmp_cache_creator;
CREATE TABLE tmp_cache_creator (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS disk = disk(
    type = cache,
    disk = 'local_disk',
    name = '04054_cache_creator',
    path = '04054_borrow_test_cache/',
    max_size = '100Mi',
    load_metadata_asynchronously = 0
);

-- The filesystem cache '04054_cache_creator' should now exist.
SELECT cache_name FROM system.filesystem_cache_settings WHERE cache_name = '04054_cache_creator';

-- Create a table that borrows space from that cache.
DROP TABLE IF EXISTS tmp_borrowed;
CREATE TABLE tmp_borrowed (key UInt64, value String)
ENGINE = MergeTree() ORDER BY key
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = 'borrow_from_cache',
    metadata_type = 'memory',
    cache_name = '04054_cache_creator',
    name = '04054_borrowed_disk'
);

-- Insert data
INSERT INTO tmp_borrowed VALUES (1, 'hello'), (2, 'world'), (3, 'test');

-- Read it back
SELECT * FROM tmp_borrowed ORDER BY key;

-- Verify we can do more operations
INSERT INTO tmp_borrowed VALUES (4, 'another'), (5, 'row');
SELECT count() FROM tmp_borrowed;

-- Clean up
DROP TABLE tmp_borrowed;
DROP TABLE tmp_cache_creator;
