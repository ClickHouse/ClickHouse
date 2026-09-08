#!/usr/bin/env bash
# The `.backup` manifest is XML, so every user-controlled string in it has to be escaped. A value that is not
# escaped still produces a `BACKUP_CREATED`, and the damage only shows up at restore time as a SAX parse error,
# with no way back to the data - so each of these asserts that the backup can actually be read again.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS tbl;
CREATE TABLE tbl (a Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tbl VALUES (1), (2), (3);
"

# `SETTINGS id` is written to the manifest as `<backup_id>`.
backup_with_id="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_id')"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE tbl TO ${backup_with_id} SETTINGS id = 'a&b<c>d\"e'" > /dev/null
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE tbl AS tbl_from_id FROM ${backup_with_id}" > /dev/null
${CLICKHOUSE_CLIENT} --query "SELECT 'backup_id', sum(a) FROM tbl_from_id"

# The base backup's locator is written to the incremental backup's manifest as `<base_backup>`, so a `&` in the
# base backup's path reaches the manifest of the backup that refers to it.
base_backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_base&1')"
incremental_backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_incremental')"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE tbl TO ${base_backup}" > /dev/null
${CLICKHOUSE_CLIENT} --query "INSERT INTO tbl VALUES (4)"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE tbl TO ${incremental_backup} SETTINGS base_backup = ${base_backup}" > /dev/null
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE tbl AS tbl_from_incremental FROM ${incremental_backup}" > /dev/null
${CLICKHOUSE_CLIENT} --query "SELECT 'base_backup', sum(a) FROM tbl_from_incremental"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE tbl;
DROP TABLE tbl_from_id;
DROP TABLE tbl_from_incremental;
"
