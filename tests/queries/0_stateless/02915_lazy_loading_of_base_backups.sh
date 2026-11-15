#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

a_backup_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_a
a_backup="Disk('backups', '$a_backup_id')"

b_backup_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_b
b_backup="Disk('backups', '$b_backup_id')"

c_backup_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_c
c_backup="Disk('backups', '$c_backup_id')"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS tbl1;
DROP TABLE IF EXISTS tbl2;
DROP TABLE IF EXISTS tbl3;
"

${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE tbl1 (a Int32) ENGINE = MergeTree() ORDER BY tuple();
"

# The following BACKUP command must write backup 'a'.
${CLICKHOUSE_CLIENT} -m --query "
BACKUP DATABASE ${CLICKHOUSE_DATABASE} TO $a_backup SETTINGS id='$a_backup_id';
" | grep -o "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE tbl2 (a Int32) ENGINE = MergeTree() ORDER BY tuple();
"

# The following BACKUP command must read backup 'a' and write backup 'b'.
${CLICKHOUSE_CLIENT} -m --query "
BACKUP DATABASE ${CLICKHOUSE_DATABASE} TO $b_backup SETTINGS id='$b_backup_id', base_backup=$a_backup;
" | grep -o "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE tbl3 (a Int32) ENGINE = MergeTree() ORDER BY tuple();
"

# The following BACKUP command must read only backup 'b' (and not 'a') and write backup 'c'.
${CLICKHOUSE_CLIENT} -m --query "
BACKUP DATABASE ${CLICKHOUSE_DATABASE} TO $c_backup SETTINGS id='$c_backup_id', base_backup=$b_backup;
" | grep -o "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE tbl1;
DROP TABLE tbl2;
DROP TABLE tbl3;
"

r1_restore_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_r1
r2_restore_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_r2
r3_restore_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_r3

# The following RESTORE command must read all 3 backups 'a', 'b', c' because the table 'tbl1' was in the first backup.
${CLICKHOUSE_CLIENT} -m --query "
RESTORE TABLE ${CLICKHOUSE_DATABASE}.tbl1 FROM $c_backup SETTINGS id='$r1_restore_id';
" | grep -o "RESTORED"

# The following RESTORE command must read only 2 backups 'b', c' (and not 'a') because the table 'tbl2' was in the second backup.
${CLICKHOUSE_CLIENT} -m --query "
RESTORE TABLE ${CLICKHOUSE_DATABASE}.tbl2 FROM $c_backup SETTINGS id='$r2_restore_id';
" | grep -o "RESTORED"

# The following RESTORE command must read only 1 backup 'c' (and not 'a' or 'b') because the table 'tbl3' was in the third backup.
${CLICKHOUSE_CLIENT} -m --query "
RESTORE TABLE ${CLICKHOUSE_DATABASE}.tbl3 FROM $c_backup SETTINGS id='$r3_restore_id';
" | grep -o "RESTORED"

all_ids="['$a_backup_id', '$b_backup_id', '$c_backup_id', '$r1_restore_id', '$r2_restore_id', '$r3_restore_id']"
id_prefix_len=`expr "${CLICKHOUSE_TEST_UNIQUE_NAME}_" : '.*'`

${CLICKHOUSE_CLIENT} -m --query "
SELECT substr(id, 1 + $id_prefix_len) as short_id, ProfileEvents['BackupsOpenedForRead'], ProfileEvents['BackupsOpenedForWrite'] FROM system.backups WHERE id IN ${all_ids} ORDER BY short_id
"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE tbl1;
DROP TABLE tbl2;
DROP TABLE tbl3;
"
