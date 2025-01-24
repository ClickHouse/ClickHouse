#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
  DROP TABLE IF EXISTS data;
  DROP TABLE IF EXISTS data_1;
  DROP TABLE IF EXISTS data_2;
  CREATE TABLE data (key Int) ENGINE=MergeTree() ORDER BY tuple();
  INSERT INTO data SELECT * from numbers(10);
"

echo 'use_same_password_for_base_backup'
echo "base"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE data TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_base.zip') SETTINGS password='password';" | cut -f2

echo 'add_more_data_1'
$CLICKHOUSE_CLIENT -q "INSERT INTO data SELECT * FROM numbers(10,10);"

echo "inc_1"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE data TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_inc_1.zip') SETTINGS base_backup=Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_base.zip'),password='password',use_same_password_for_base_backup=1" | cut -f2

echo 'add_more_data_2'
$CLICKHOUSE_CLIENT -q "INSERT INTO data SELECT * FROM numbers(20,10);"

echo "inc_2"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE data TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_inc_2.zip') SETTINGS base_backup=Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_inc_1.zip'),password='password',use_same_password_for_base_backup=1" | cut -f2

echo "inc_2_bad"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE data TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_inc_2_bad.zip') SETTINGS base_backup=Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_inc_1.zip'),password='password'" |& grep -m1 -o "Couldn't unpack zip archive '${CLICKHOUSE_TEST_UNIQUE_NAME}_inc_1.zip': Password is required. (CANNOT_UNPACK_ARCHIVE)"

echo "restore_inc_1"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE data AS data_1 FROM Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_inc_1.zip') SETTINGS password='password',use_same_password_for_base_backup=1" | cut -f2

echo "restore_inc_2"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE data AS data_2 FROM Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_inc_2.zip') SETTINGS password='password',use_same_password_for_base_backup=1" | cut -f2

echo "restore_inc_2_bad"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE data AS data_2 FROM Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_inc_2.zip') SETTINGS password='password'" |& grep -m1 -o "Couldn't unpack zip archive '${CLICKHOUSE_TEST_UNIQUE_NAME}_inc_1.zip': Password is required. (CANNOT_UNPACK_ARCHIVE)"

echo "count_inc_1"
$CLICKHOUSE_CLIENT -q "SELECT COUNT(*) FROM data_1" | cut -f2

echo "count_inc_2"
$CLICKHOUSE_CLIENT -q "SELECT COUNT(*) FROM data_2" | cut -f2

exit 0
