#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
    DROP DATABASE IF EXISTS 02911_keeper_map;
    CREATE DATABASE 02911_keeper_map;
    CREATE TABLE 02911_keeper_map.02911_backup_restore_keeper_map1 (key UInt64, value String) Engine=KeeperMap('/' || currentDatabase() || '/test02911') PRIMARY KEY key;
    CREATE TABLE 02911_keeper_map.02911_backup_restore_keeper_map2 (key UInt64, value String) Engine=KeeperMap('/' || currentDatabase() || '/test02911') PRIMARY KEY key;
    CREATE TABLE 02911_keeper_map.02911_backup_restore_keeper_map3 (key UInt64, value String) Engine=KeeperMap('/' || currentDatabase() || '/test02911_different') PRIMARY KEY key;

    INSERT INTO 02911_keeper_map.02911_backup_restore_keeper_map2 SELECT number, 'test' || toString(number) FROM system.numbers LIMIT 5000;
    INSERT INTO 02911_keeper_map.02911_backup_restore_keeper_map3 SELECT number, 'test' || toString(number) FROM system.numbers LIMIT 3000;
"

backup_path="$CLICKHOUSE_DATABASE/02911_keeper_map"
for i in $(seq 1 3); do
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM 02911_keeper_map.02911_backup_restore_keeper_map$i;"
done

$CLICKHOUSE_CLIENT -q "BACKUP DATABASE 02911_keeper_map TO Disk('backups', '$backup_path');" > /dev/null

$CLICKHOUSE_CLIENT -q "DROP DATABASE 02911_keeper_map SYNC;"

for i in $(seq 1 3); do
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM 02911_keeper_map.02911_backup_restore_keeper_map$i;" 2>&1 | grep -Fq "UNKNOWN_DATABASE" && echo 'OK' || echo 'ERROR'
done

$CLICKHOUSE_CLIENT -q "RESTORE DATABASE 02911_keeper_map FROM Disk('backups', '$backup_path');" > /dev/null

for i in $(seq 1 3); do
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM 02911_keeper_map.02911_backup_restore_keeper_map$i;"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE 02911_keeper_map.02911_backup_restore_keeper_map3 SYNC;"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM 02911_keeper_map.02911_backup_restore_keeper_map3;" 2>&1 | grep -Fq "UNKNOWN_TABLE" && echo 'OK' || echo 'ERROR'

$CLICKHOUSE_CLIENT -q "RESTORE TABLE 02911_keeper_map.02911_backup_restore_keeper_map3 FROM Disk('backups', '$backup_path');" > /dev/null

for i in $(seq 1 3); do
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM 02911_keeper_map.02911_backup_restore_keeper_map$i;"
done

$CLICKHOUSE_CLIENT -q "DROP DATABASE 02911_keeper_map SYNC;"