#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# In this test we restore from "/tests/queries/0_stateless/backups/with_broken_part.zip"
backup_name="$($CURDIR/helpers/install_predefined_backup.sh with_broken_part.zip)"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS tbl"

# First try to restore with the setting `restore_broken_parts_as_detached` set to false.
$CLICKHOUSE_CLIENT --query "RESTORE TABLE default.tbl AS tbl FROM Disk('backups', '${backup_name}')" 2>&1 | tr -d \\n | grep "data.bin doesn't exist" | grep "while restoring part all_2_2_0" > /dev/null && echo "OK" || echo "FAILED"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS tbl"

# Then try to restore with the setting `restore_broken_parts_as_detached` set to true.
$CLICKHOUSE_CLIENT --query "RESTORE TABLE default.tbl AS tbl FROM Disk('backups', '${backup_name}') SETTINGS restore_broken_parts_as_detached = true" 2>/dev/null | awk -F '\t' '{print $2}'

$CLICKHOUSE_CLIENT <<EOF
SELECT * FROM tbl ORDER BY x;
SELECT name, reason FROM system.detached_parts WHERE database = currentDatabase() AND table = 'tbl';

DROP TABLE tbl;
EOF
