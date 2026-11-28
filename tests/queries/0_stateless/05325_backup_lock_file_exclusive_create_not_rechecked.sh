#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: enables global failpoints

# Regression test: a `File(...)` backup creates its `.lock` file with an `O_EXCL` create, so a lock
# write that succeeds already proves the destination is ours. The verification read that used to follow
# the lock file creation must not run on that clean path: it cannot tell anything new, and it can fail on
# its own. Only `backup_fail_lock_file_check_after_creation` is enabled here, so the backup goes through
# only if that read is skipped. The late-failure recovery path is covered by
# `05052_backup_lock_file_verified_once`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

backup_id=${CLICKHOUSE_TEST_UNIQUE_NAME}
backup="File('$backup_id')"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS t;
CREATE TABLE t (x Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t VALUES (1), (2), (3);
"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT backup_fail_lock_file_check_after_creation"

${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $backup SETTINGS id='$backup_id'" | grep -o "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT backup_fail_lock_file_check_after_creation"

# The backup is usable, and no lock was left behind: restoring from it must read all three rows.
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.t AS ${CLICKHOUSE_DATABASE}.t_restored FROM $backup" | grep -o "RESTORED"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM ${CLICKHOUSE_DATABASE}.t_restored"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t;
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_restored;
"
