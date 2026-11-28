#!/usr/bin/env bash
# Tags: no-fasttest, no-encrypted-storage

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

database_name="$CLICKHOUSE_DATABASE"_03276_test_database
${CLICKHOUSE_CLIENT} "
DROP DATABASE IF EXISTS $database_name;
CREATE DATABASE $database_name;
"

${CLICKHOUSE_CLIENT} "
CREATE TABLE $database_name.test_table (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
INSERT INTO $database_name.test_table SELECT number, number FROM numbers(15000);
SELECT (id % 10) AS key, count() FROM $database_name.test_table GROUP BY key ORDER BY key;
"

${CLICKHOUSE_CLIENT} --query="BACKUP TABLE $database_name.test_table TO File('$database_name.test_table') FORMAT Null"

${CLICKHOUSE_CLIENT} --query="SELECT '--'"

backup_database_name="$CLICKHOUSE_DATABASE"_03276_test_table_backup_database

${CLICKHOUSE_CLIENT} --query="DROP DATABASE IF EXISTS $backup_database_name"

${CLICKHOUSE_CLIENT} --query="CREATE DATABASE $backup_database_name ENGINE = Backup('$database_name', File('$database_name.test_table'))"

${CLICKHOUSE_CLIENT} --query="SELECT (id % 10) AS key, count() FROM $backup_database_name.test_table GROUP BY key ORDER BY key"

${CLICKHOUSE_CLIENT} "
DROP DATABASE $backup_database_name;
DROP DATABASE $database_name;
"
