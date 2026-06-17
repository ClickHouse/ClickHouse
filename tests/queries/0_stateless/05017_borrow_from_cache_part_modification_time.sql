-- Tags: no-fasttest, no-replicated-database

-- Regression test: a part stored on a `borrow_from_cache` object storage with `memory` metadata
-- must report a real `modification_time`, not the 1970 epoch. `MergeTree` reads the part
-- `modification_time` from the part directory mtime (`DataPartStorageOnDiskBase::getLastModified`),
-- which it sets on the temp part directory before moving it into place. The in-memory metadata
-- storage used to ignore directory timestamps, so parts reported `1970-01-01`.

-- First, create a filesystem cache by making a cached disk.
DROP TABLE IF EXISTS tmp_cache_creator;
CREATE TABLE tmp_cache_creator (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS disk = disk(
    type = cache,
    disk = 'local_disk',
    name = '05017_cache_creator',
    path = '05017_borrow_modtime_cache/',
    max_size = '100Mi',
    load_metadata_asynchronously = 0
);

-- Create a table that borrows space from that cache.
DROP TABLE IF EXISTS tmp_borrowed;
CREATE TABLE tmp_borrowed (key UInt64, value String)
ENGINE = MergeTree() ORDER BY key
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = 'borrow_from_cache',
    metadata_type = 'memory',
    cache_name = '05017_cache_creator',
    name = '05017_borrowed_modtime_disk'
);

INSERT INTO tmp_borrowed VALUES (1, 'hello'), (2, 'world'), (3, 'test');

-- The part's modification_time must be a real timestamp set during the insert, not the epoch.
SELECT modification_time > '2020-01-01 00:00:00'
FROM system.parts
WHERE database = currentDatabase() AND table = 'tmp_borrowed' AND active
ORDER BY name;

-- A merge writes a new part the same way (set mtime, then move into place); it must also be real.
INSERT INTO tmp_borrowed VALUES (4, 'another'), (5, 'row');
OPTIMIZE TABLE tmp_borrowed FINAL;

SELECT modification_time > '2020-01-01 00:00:00'
FROM system.parts
WHERE database = currentDatabase() AND table = 'tmp_borrowed' AND active
ORDER BY name;

DROP TABLE tmp_borrowed;
DROP TABLE tmp_cache_creator;
