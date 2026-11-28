#!/usr/bin/env bash
# The `.backup` manifest is XML, so a user-controlled string in it has to be escaped - unescaped, it is only
# found unreadable at restore time.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS tbl;
CREATE TABLE tbl (a Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tbl VALUES (1), (2), (3);
"

# `SETTINGS id` becomes `<backup_id>`. The id has to be unique server-wide: `BackupsWorker` rejects one it
# has already seen.
backup_with_id="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_id')"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE tbl TO ${backup_with_id} SETTINGS id = 'a&b<c>d\"e ${CLICKHOUSE_TEST_UNIQUE_NAME}'" > /dev/null
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE tbl AS tbl_from_id FROM ${backup_with_id}" > /dev/null
${CLICKHOUSE_CLIENT} --query "SELECT 'backup_id', sum(a) FROM tbl_from_id"

# An incremental backup writes the base backup's locator to `<base_backup>`, so a `&` in the base backup's
# path reaches its manifest.
base_backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_base&1')"
incremental_backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_incremental')"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE tbl TO ${base_backup}" > /dev/null
${CLICKHOUSE_CLIENT} --query "INSERT INTO tbl VALUES (4)"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE tbl TO ${incremental_backup} SETTINGS base_backup = ${base_backup}" > /dev/null
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE tbl AS tbl_from_incremental FROM ${incremental_backup}" > /dev/null
${CLICKHOUSE_CLIENT} --query "SELECT 'base_backup', sum(a) FROM tbl_from_incremental"

# The check walks the string as UTF-8, so the rest of UTF-8 has to keep working: a multi-byte id is legal
# XML and must still round-trip.
backup_with_utf8="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_utf8')"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE tbl TO ${backup_with_utf8} SETTINGS id = 'привет 🙂 ${CLICKHOUSE_TEST_UNIQUE_NAME}'" > /dev/null
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE tbl AS tbl_from_utf8 FROM ${backup_with_utf8}" > /dev/null
${CLICKHOUSE_CLIENT} --query "SELECT 'utf8_id', sum(a) FROM tbl_from_utf8"

# XML cannot carry a C0 control at all, so escaping cannot help and the backup has to fail. A SQL literal
# decodes `\0` and `\v` into one.
control_nul="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_control_nul')"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE tbl TO ${control_nul} SETTINGS id = 'a\0b ${CLICKHOUSE_TEST_UNIQUE_NAME}'" 2>&1 \
    | grep -qF 'XML cannot carry unchanged' && echo -e "control_char_nul\trejected" || echo -e "control_char_nul\tNOT rejected"

control_vtab="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_control_vtab')"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE tbl TO ${control_vtab} SETTINGS id = 'a\vb ${CLICKHOUSE_TEST_UNIQUE_NAME}'" 2>&1 \
    | grep -qF 'XML cannot carry unchanged' && echo -e "control_char_vtab\trejected" || echo -e "control_char_vtab\tNOT rejected"

# A carriage return is legal XML, but a parser rewrites it to a line feed on read, so the value would read
# back different. Refused for that.
control_cr="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_control_cr')"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE tbl TO ${control_cr} SETTINGS id = 'a\rb ${CLICKHOUSE_TEST_UNIQUE_NAME}'" 2>&1 \
    | grep -qF 'XML cannot carry unchanged' && echo -e "carriage_return\trejected" || echo -e "carriage_return\tNOT rejected"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE tbl;
DROP TABLE tbl_from_id;
DROP TABLE tbl_from_incremental;
DROP TABLE tbl_from_utf8;
"
