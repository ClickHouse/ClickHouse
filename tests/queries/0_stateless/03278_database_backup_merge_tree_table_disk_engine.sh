#!/usr/bin/env bash
# Tags: no-fasttest, no-encrypted-storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

database_name="$CLICKHOUSE_DATABASE"_03278_test_database
backup_database_name="$CLICKHOUSE_DATABASE"_03278_test_table_backup_database
backup_path="$CLICKHOUSE_DATABASE"_03278_test_database.test_table

$CLICKHOUSE_CLIENT "
DROP DATABASE IF EXISTS $database_name;
CREATE DATABASE $database_name;

CREATE TABLE $database_name.test_table (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO $database_name.test_table SELECT number, number FROM numbers(15000);

SELECT (id % 10) AS key, count() FROM $database_name.test_table GROUP BY key ORDER BY key;

BACKUP TABLE $database_name.test_table TO Disk('backups', '$backup_path') FORMAT Null;

SELECT '--';

DROP DATABASE IF EXISTS $backup_database_name;
CREATE DATABASE $backup_database_name ENGINE = Backup('$database_name', Disk('backups', '$backup_path'));

SELECT (id % 10) AS key, count() FROM $backup_database_name.test_table GROUP BY key ORDER BY key;

DROP DATABASE $backup_database_name;

DROP DATABASE $database_name;
"
