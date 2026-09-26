#!/usr/bin/env bash
# Tags: no-distributed-cache

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

settings=(
    # It does not work (fixme)
    --min_bytes_to_use_direct_io='1Gi'
    # ui_uring local_fs_method does not work here (fixme)
    --local_filesystem_read_method='pread'
)

$CLICKHOUSE_CLIENT "${settings[@]}" -nm -q "
    DROP TABLE IF EXISTS test;

    CREATE TABLE test (a Int32, b String)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS disk = disk(
        type = 'local_blob_storage',
        path = '${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}/');

    INSERT INTO test SELECT 1, 'test';
    SELECT * FROM test;

    DROP TABLE test SYNC;

    CREATE TABLE test (a Int32, b String)
    ENGINE = MergeTree() ORDER BY tuple()
    SETTINGS disk = disk(
        type = 'cache',
        max_size = '10Mi',
        path = '${CLICKHOUSE_TEST_UNIQUE_NAME}/',
        disk = disk(type='local_blob_storage', path='${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}/'));

    INSERT INTO test SELECT 1, 'test';
    SELECT * FROM test;

    DROP TABLE test SYNC;
"
