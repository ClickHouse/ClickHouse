-- Tags: no-fasttest, no-replicated-database

CREATE TABLE test2 (a Int32, b String)
ENGINE = MergeTree() ORDER BY a SETTINGS disk = disk(type = cache, disk = 'local_disk', name = '$CLICHOUSE_TEST_UNIQUE_NAME_2', cache_name='cache_collection');
DESCRIBE FILESYSTEM CACHE '$CLICHOUSE_TEST_UNIQUE_NAME_2';
