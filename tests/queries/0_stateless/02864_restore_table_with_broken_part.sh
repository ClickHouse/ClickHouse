#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Copy backups/with_broken_part.zip into the disk named "backups".
SRC_BACKUP_DIR=$CURDIR/backups
SRC_BACKUP_FILENAME=with_broken_part.zip

BACKUPS_DISK=backups
BACKUPS_DIR=$($CLICKHOUSE_CLIENT --query "SELECT path FROM system.disks WHERE name='$BACKUPS_DISK'")

if [ -z "$BACKUPS_DIR" ]; then
    echo Disk \'$BACKUPS_DISK\' not found
    exit 1
fi

BACKUP_FILENAME=$CLICKHOUSE_DATABASE/${SRC_BACKUP_FILENAME}
BACKUP_NAME="Disk('$BACKUPS_DISK', '$BACKUP_FILENAME')"

mkdir -p "$(dirname "$BACKUPS_DIR/$BACKUP_FILENAME")"
ln -s "$SRC_BACKUP_DIR/$SRC_BACKUP_FILENAME" "$BACKUPS_DIR/$BACKUP_FILENAME"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS tbl"

# First try to restore with the setting `restore_broken_parts_as_detached` set to false.
$CLICKHOUSE_CLIENT --query "RESTORE TABLE default.tbl AS tbl FROM $BACKUP_NAME" 2>&1 | tr -d \\n | grep "data.bin doesn't exist" | grep "while restoring part all_2_2_0" > /dev/null && echo "OK" || echo "FAILED"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS tbl"

# Then try to restore with the setting `restore_broken_parts_as_detached` set to true.
$CLICKHOUSE_CLIENT --query "RESTORE TABLE default.tbl AS tbl FROM $BACKUP_NAME SETTINGS restore_broken_parts_as_detached = true" 2>/dev/null | awk -F '\t' '{print $2}'

$CLICKHOUSE_CLIENT --multiquery <<EOF
SELECT * FROM tbl ORDER BY x;
SELECT name, reason FROM system.detached_parts WHERE database = currentDatabase() AND table = 'tbl';

DROP TABLE tbl;
EOF
