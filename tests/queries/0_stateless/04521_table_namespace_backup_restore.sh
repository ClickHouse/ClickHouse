#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=$CLICKHOUSE_DATABASE

$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE \`ns.src\` (x UInt32) ENGINE = MergeTree ORDER BY x;
    INSERT INTO \`ns.src\` VALUES (1), (2);
"

echo "-- BACKUP a table by its namespace path"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE ns.src TO Disk('backups', '${db}_ns_dir')" | grep -m1 -c "BACKUP_CREATED"

echo "-- RESTORE it under a different namespace-path name"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE ns.src AS ns.restored FROM Disk('backups', '${db}_ns_dir')" | grep -m1 -c "RESTORED"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM \`ns.restored\`"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE $db.\`ns.restored\`"

echo "-- a restore source names the backup contents, not the live namespace scope"
$CLICKHOUSE_CLIENT -m -q "
    CREATE DATABASE ${db}_old;
    CREATE TABLE ${db}_old.t (x UInt8) ENGINE = Memory;
    INSERT INTO ${db}_old.t VALUES (5);
    BACKUP TABLE ${db}_old.t TO Disk('backups', '${db}_old_dir');
    DROP DATABASE ${db}_old;
" | grep -m1 -c "BACKUP_CREATED"
$CLICKHOUSE_CLIENT -q "RESTORE TABLE ${db}_old.t FROM Disk('backups', '${db}_old_dir')" 2>&1 | grep -m1 -c "UNKNOWN_DATABASE"

echo "-- RESTORE DATABASE honors namespace-path exclusions"
$CLICKHOUSE_CLIENT -q "BACKUP DATABASE $db TO Disk('backups', '${db}_full_dir')" | grep -m1 -c "BACKUP_CREATED"
$CLICKHOUSE_CLIENT -q "RESTORE DATABASE $db AS ${db}_restored EXCEPT TABLE ns.src FROM Disk('backups', '${db}_full_dir')" | grep -m1 -c "RESTORED"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE ${db}_restored.\`ns.src\`"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE ${db}_restored.\`ns.restored\`"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${db}_restored"

echo "-- EXCEPT TABLE folds a namespace path but keeps rejecting real databases"
$CLICKHOUSE_CLIENT -q "BACKUP DATABASE $db EXCEPT TABLE ns.src TO Disk('backups', '${db}_except_dir')" | grep -m1 -c "BACKUP_CREATED"
$CLICKHOUSE_CLIENT -m -q "
    CREATE DATABASE IF NOT EXISTS ${db}_other;
    BACKUP DATABASE $db EXCEPT TABLE ${db}_other.t TO Disk('backups', '${db}_bad_dir');
" 2>&1 | grep -m1 -c "doesn't match the database name"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${db}_other"
