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

backup_name="$(install_test_backup with_broken_part.zip)"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS tbl"

# First try to restore with the setting `restore_broken_parts_as_detached` set to false.
$CLICKHOUSE_CLIENT --query "RESTORE TABLE default.tbl AS tbl FROM Disk('backups', '${backup_name}')" 2>&1 | tr -d \\n | grep "data.bin doesn't exist" | grep "while restoring part all_2_2_0" > /dev/null && echo "OK" || echo "FAILED"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS tbl"

# Then try to restore with the setting `restore_broken_parts_as_detached` set to true.
$CLICKHOUSE_CLIENT --query "RESTORE TABLE default.tbl AS tbl FROM Disk('backups', '${backup_name}') SETTINGS restore_broken_parts_as_detached = true" 2>/dev/null | awk -F '\t' '{print $2}'

$CLICKHOUSE_CLIENT --multiquery <<EOF
SELECT * FROM tbl ORDER BY x;
SELECT name, reason FROM system.detached_parts WHERE database = currentDatabase() AND table = 'tbl';

DROP TABLE tbl;
EOF
