#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: enables a global failpoint

# Regression for the empty-data_file_name guard in tryRemoveAllFiles on a non-memory writer.
# An unchanged (fully base-backed) incremental entry has an empty data_file_name. Without the guard,
# cleanup of a failed backup calls removeFile("") which resolves to the backup root (root_path / "");
# on the Disk/File writer that unlinks the backup directory (EISDIR) and throws, aborting cleanup and
# leaving the failed backup behind so its destination cannot be reused.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

base_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_base
inc_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_inc
base_backup="Disk('backups', '$base_id')"
inc_backup="Disk('backups', '$inc_id')"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS t;
CREATE TABLE t (x Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t VALUES (1), (2), (3);
"

# Full base backup.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $base_backup SETTINGS id='$base_id'" | grep -o "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT backup_fail_before_writing_metadata"

# Unchanged incremental: every entry is fully base-backed, so each has an empty data_file_name. The
# backup fails before finalization; cleanup must skip the empty names instead of unlinking the root.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $inc_backup SETTINGS id='$inc_id', base_backup=$base_backup" 2>&1 | grep -o "FAULT_INJECTED" | head -n1

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT backup_fail_before_writing_metadata"

# The destination must be free again: re-using it must succeed (it would fail with an "already exists"
# error if cleanup had aborted on removeFile("") and left the failed backup behind).
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $inc_backup SETTINGS id='${inc_id}_retry', base_backup=$base_backup" | grep -o "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t"
