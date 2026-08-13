#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

database_name="$CLICKHOUSE_DATABASE"_02911_keeper_map
$CLICKHOUSE_CLIENT -m -q "
    DROP DATABASE IF EXISTS $database_name;
    CREATE DATABASE $database_name;
    CREATE TABLE $database_name.02911_backup_restore_keeper_map1 (key UInt64, value String) Engine=KeeperMap('/' || currentDatabase() || '/test02911') PRIMARY KEY key;
    CREATE TABLE $database_name.02911_backup_restore_keeper_map2 (key UInt64, value String) Engine=KeeperMap('/' || currentDatabase() || '/test02911') PRIMARY KEY key; -- table using same Keeper path as 02911_backup_restore_keeper_map1
    CREATE TABLE $database_name.02911_backup_restore_keeper_map3 (key UInt64, value String) Engine=KeeperMap('/' || currentDatabase() || '/test02911_different') PRIMARY KEY key;
"

$CLICKHOUSE_CLIENT -m -q "INSERT INTO $database_name.02911_backup_restore_keeper_map2 SELECT number, 'test' || toString(number) FROM system.numbers LIMIT 5000;"

$CLICKHOUSE_CLIENT -m -q "INSERT INTO $database_name.02911_backup_restore_keeper_map3 SELECT number, 'test' || toString(number) FROM system.numbers LIMIT 3000;"

backup_path="$database_name"
for i in $(seq 1 3); do
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM $database_name.02911_backup_restore_keeper_map$i;"
done

$CLICKHOUSE_CLIENT -q "BACKUP DATABASE $database_name TO Disk('backups', '$backup_path');" > /dev/null

$CLICKHOUSE_CLIENT -q "DROP DATABASE $database_name SYNC;"

for i in $(seq 1 3); do
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM $database_name.02911_backup_restore_keeper_map$i;" 2>&1 | grep -Fq "UNKNOWN_DATABASE" && echo 'OK' || echo 'ERROR'
done

$CLICKHOUSE_CLIENT -q "RESTORE DATABASE $database_name FROM Disk('backups', '$backup_path');" > /dev/null

for i in $(seq 1 3); do
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM $database_name.02911_backup_restore_keeper_map$i;"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE $database_name.02911_backup_restore_keeper_map3 SYNC;"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM $database_name.02911_backup_restore_keeper_map3;" 2>&1 | grep -Fq "UNKNOWN_TABLE" && echo 'OK' || echo 'ERROR'

$CLICKHOUSE_CLIENT -q "RESTORE TABLE $database_name.02911_backup_restore_keeper_map3 FROM Disk('backups', '$backup_path');" > /dev/null

for i in $(seq 1 3); do
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM $database_name.02911_backup_restore_keeper_map$i;"
done

$CLICKHOUSE_CLIENT -q "DROP DATABASE $database_name SYNC;"
