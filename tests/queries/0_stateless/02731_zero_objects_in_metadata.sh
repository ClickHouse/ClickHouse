#!/usr/bin/env bash
# Tags: no-fasttest, no-object-storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for DISK in s3_disk s3_cache
do
    BACKUP_NAME="test_s3_backup_${CLICKHOUSE_TEST_UNIQUE_NAME}_${DISK}"
    BACKUP_DB_NAME="test_backup_db_${CLICKHOUSE_DATABASE}"

    ${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS test;
    CREATE TABLE test (id Int32, empty Array(Int32))
        ENGINE=MergeTree ORDER BY id
        SETTINGS min_rows_for_wide_part=0, min_bytes_for_wide_part=0, disk='$DISK';

    INSERT INTO test (id) VALUES (1);
    SELECT * FROM test;

    BACKUP TABLE test TO Disk('backups', '${BACKUP_NAME}') FORMAT Null;
    DROP TABLE test;
    RESTORE TABLE test FROM Disk('backups', '${BACKUP_NAME}') FORMAT Null;

    SELECT * FROM test;
    SELECT empty FROM test;

    DROP DATABASE IF EXISTS "${BACKUP_DB_NAME}";
    -- we create a backup db that points to the current test's CLICKHOUSE_DATABASE
    -- otherwise it will have no tables.
    CREATE DATABASE "${BACKUP_DB_NAME}"
        ENGINE=Backup('${CLICKHOUSE_DATABASE}', Disk('backups', '${BACKUP_NAME}'));

    SELECT * FROM "${BACKUP_DB_NAME}".test;
    SELECT empty FROM "${BACKUP_DB_NAME}".test;

    DROP DATABASE "${BACKUP_DB_NAME}";
    "
done
