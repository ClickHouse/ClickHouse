#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: enables global failpoints

# Regression test: when the conditional write of the `.lock` file reports a failure after it had
# committed, `createLockFile` reads the lock back and finds this attempt's own contents in it. That
# already proves the destination is ours, so the immediate verification read that used to follow must
# not run: it can fail on its own, and it would then abort an uncontended backup and leave behind a
# lock that cannot be removed, because removing it needs a readable lock too.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

backup_id=${CLICKHOUSE_TEST_UNIQUE_NAME}
backup="Disk('backups', '$backup_id')"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS t;
CREATE TABLE t (x Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t VALUES (1), (2), (3);
"

# The lock file is written and the write then reports a failure, and the verification read that would
# follow the lock file creation fails as well.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT backup_fail_lock_file_write_after_commit"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT backup_fail_lock_file_check_after_creation"

# The lock was read back once already, so the backup must go through.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $backup SETTINGS id='$backup_id'" | grep -o "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT backup_fail_lock_file_write_after_commit"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT backup_fail_lock_file_check_after_creation"

# The backup is usable, and no lock was left behind: restoring from it must read all three rows.
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.t AS ${CLICKHOUSE_DATABASE}.t_restored FROM $backup" | grep -o "RESTORED"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM ${CLICKHOUSE_DATABASE}.t_restored"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t;
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_restored;
"
