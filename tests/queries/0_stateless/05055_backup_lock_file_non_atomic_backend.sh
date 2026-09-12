#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: enables a global failpoint

# A lock file holding this attempt's own contents proves the attempt wrote it only when the lock write
# could not have replaced somebody else's lock. `Disk(...)` writes the lock in rewrite mode, so a second
# backup can overwrite the lock of the backup that got to the destination first and then read its own
# contents back. On such a backend the destination must keep being reported as taken, instead of being
# taken over by the attempt that clobbered the lock -- and the lock must stay in place too: removing it
# on the way out would unfence the backup it may have been written over (see
# `05153_backup_lock_file_cleanup_keeps_foreign_fence` for the contended case).

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

# The same missing proof means the lock is not this attempt's to remove either: it stays, and the
# destination keeps being reported as taken by the next backup that tries it.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $backup SETTINGS id='${backup_id}_again'" 2>&1 | grep -o "BACKUP_ALREADY_EXISTS" | head -n1

# Nothing else is affected: a backup to another destination on the same disk goes through.
other_backup="Disk('backups', '${backup_id}_other')"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $other_backup SETTINGS id='${backup_id}_other'" | grep -o "BACKUP_CREATED"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.t AS ${CLICKHOUSE_DATABASE}.t_restored FROM $other_backup" | grep -o "RESTORED"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM ${CLICKHOUSE_DATABASE}.t_restored"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t;
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_restored;
"
