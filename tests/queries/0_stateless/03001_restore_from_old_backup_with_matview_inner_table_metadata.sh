#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Copies a test predefined backup from "/tests/queries/0_stateless/backups/" folder to the "backups" disk,
# returns the path to the backup relative to that disk.
function install_test_backup()
{
    local test_backup_filename="$1"
    local test_backup_path="$CURDIR/backups/${test_backup_filename}"

    local backups_disk_root
    backups_disk_root=$($CLICKHOUSE_CLIENT --query "SELECT path FROM system.disks WHERE name='backups'")

    if [ -z "${backups_disk_root}" ]; then
        echo "Disk '${backups_disk_root}' not found"
        exit 1
    fi

    local install_path=${backups_disk_root}/${CLICKHOUSE_DATABASE}/${test_backup_filename}
    mkdir -p "$(dirname "${install_path}")"
    ln -s "${test_backup_path}" "${install_path}"

    echo "${CLICKHOUSE_DATABASE}/${test_backup_filename}"
}

backup_name="$(install_test_backup old_backup_with_matview_inner_table_metadata.zip)"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS mv"
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS src"

db="$CLICKHOUSE_DATABASE"
${CLICKHOUSE_CLIENT} -q "RESTORE DATABASE mydb AS ${db} FROM Disk('backups', '${backup_name}') SETTINGS allow_different_database_def=true" | grep -o "RESTORED"

${CLICKHOUSE_CLIENT} -q "SELECT toDateTime(timestamp, 'UTC') AS ts, c12 FROM mv ORDER BY ts"

$CLICKHOUSE_CLIENT --query "DROP TABLE mv"
$CLICKHOUSE_CLIENT --query "DROP TABLE src"
