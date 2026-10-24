#!/usr/bin/env bash
# Tags: no-fasttest, no-encrypted-storage
# A backup of an altered ReplicatedMergeTree table stores the table's metadata version in a file next to its
# definition. The `Backup` database engine must still be able to open such a backup.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

database_name="${CLICKHOUSE_DATABASE}_05341_db"
backup_database_name="${CLICKHOUSE_DATABASE}_05341_backup_db"
backup_name="${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT "
DROP DATABASE IF EXISTS $database_name;
CREATE DATABASE $database_name;

CREATE TABLE $database_name.t (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') ORDER BY id;
ALTER TABLE $database_name.t ADD COLUMN value String;
INSERT INTO $database_name.t SELECT number, toString(number) FROM numbers(10);

SELECT metadata_version FROM system.tables WHERE database = '$database_name' AND name = 't';

BACKUP DATABASE $database_name TO Disk('backups', '$backup_name') FORMAT Null;

DROP DATABASE IF EXISTS $backup_database_name;
CREATE DATABASE $backup_database_name ENGINE = Backup('$database_name', Disk('backups', '$backup_name'));

SELECT count(), sum(id), max(value) FROM $backup_database_name.t;

DROP DATABASE $backup_database_name;
DROP DATABASE $database_name SYNC;
"
