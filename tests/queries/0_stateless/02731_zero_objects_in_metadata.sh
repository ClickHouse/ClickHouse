#!/usr/bin/env bash
# Tags: no-fasttest, no-s3-storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for DISK in s3_disk s3_cache
do
    ${CLICKHOUSE_CLIENT} -n --query "
    DROP TABLE IF EXISTS test;
    CREATE TABLE test (id Int32, empty Array(Int32))
        ENGINE=MergeTree ORDER BY id
        SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0, disk='$DISK';

    INSERT INTO test (id) VALUES (1);
    SELECT * FROM test;
    "

    ${CLICKHOUSE_CLIENT} -n --query "
    BACKUP TABLE test TO Disk('backups', 'test_s3_backup');
    DROP TABLE test;
    RESTORE TABLE test FROM Disk('backups', 'test_s3_backup');
    " &>/dev/null

    ${CLICKHOUSE_CLIENT} -n --query "
    SELECT * FROM test;
    SELECT empty FROM test;
    "
done
