-- Tags: no-distributed-cache

SET min_bytes_to_use_direct_io='1Gi'; -- It does not work (fixme)
SET local_filesystem_read_method='pread'; -- ui_uring local_fs_method does not work here (fixme)

DROP TABLE IF EXISTS test;

CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(
    type = 'local_blob_storage',
    path = '${CLICKHOUSE_TEST_UNIQUE_NAME}/');

INSERT INTO test SELECT 1, 'test';
SELECT * FROM test;

DROP TABLE test SYNC;

CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(
    type = 'cache',
    max_size = '10Mi',
    path = '${CLICKHOUSE_TEST_UNIQUE_NAME}/',
    disk = disk(type='local_blob_storage', path='/var/lib/clickhouse/disks/${CLICKHOUSE_TEST_UNIQUE_NAME}/'));

INSERT INTO test SELECT 1, 'test';
SELECT * FROM test;

DROP TABLE test SYNC;
