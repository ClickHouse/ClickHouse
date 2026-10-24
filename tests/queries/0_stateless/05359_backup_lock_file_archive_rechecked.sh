#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: enables global failpoints

# A `File(...)` backup creates its `.lock` exclusively, which lets a non-archive backup skip the
# verification read right after the lock creation (see `05325_backup_lock_file_exclusive_create_not_rechecked`).
# An archive backup must not skip it: `openArchive` truncates the archive before any checked write, and
# the lock can have been written over through `Disk(...)` in the meantime. With
# `backup_fail_lock_file_check_after_creation` the archive backup must therefore fail, and it must take
# its lock back, so the next backup to the same destination succeeds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

backup_id=${CLICKHOUSE_TEST_UNIQUE_NAME}
backup="File('$backup_id.zip')"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS t;
CREATE TABLE t (x Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t VALUES (1), (2), (3);
"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT backup_fail_lock_file_check_after_creation"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $backup SETTINGS id='${backup_id}_1'" 2>&1 | grep -o -m1 "FAULT_INJECTED"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT backup_fail_lock_file_check_after_creation"

${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $backup SETTINGS id='${backup_id}_2'" | grep -o "BACKUP_CREATED"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.t AS ${CLICKHOUSE_DATABASE}.t_restored FROM $backup" | grep -o "RESTORED"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM ${CLICKHOUSE_DATABASE}.t_restored"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t;
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_restored;
"
