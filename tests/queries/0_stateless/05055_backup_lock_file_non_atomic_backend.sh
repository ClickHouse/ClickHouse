#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: enables a global failpoint

# A lock file holding this attempt's own contents proves the attempt wrote it only when the lock write
# could not have replaced somebody else's lock. `Disk(...)` writes the lock in rewrite mode, so a second
# backup can overwrite the lock of the backup that got to the destination first and then read its own
# contents back. On such a backend the destination must keep being reported as taken, instead of being
# taken over by the attempt that clobbered the lock.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

backup_id=${CLICKHOUSE_TEST_UNIQUE_NAME}
backup="Disk('backups', '$backup_id')"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t;
CREATE TABLE ${CLICKHOUSE_DATABASE}.t (x Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ${CLICKHOUSE_DATABASE}.t VALUES (1), (2), (3);
"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT backup_fail_lock_file_write_after_commit"

# The lock write commits and then reports a failure. The backend has no conditional create, so the lock
# read back here is not proof of ownership: the destination is reported as taken.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $backup SETTINGS id='$backup_id'" 2>&1 | grep -o "BACKUP_ALREADY_EXISTS" | head -n1

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT backup_fail_lock_file_write_after_commit"

# Reporting the destination as taken must not leave it locked forever: the lock this attempt wrote is
# removed on the way out, so the next backup to the same destination goes through.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $backup SETTINGS id='${backup_id}_again'" | grep -o "BACKUP_CREATED"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.t AS ${CLICKHOUSE_DATABASE}.t_restored FROM $backup" | grep -o "RESTORED"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM ${CLICKHOUSE_DATABASE}.t_restored"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t;
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_restored;
"
