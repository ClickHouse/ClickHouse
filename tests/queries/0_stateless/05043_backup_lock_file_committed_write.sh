#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: enables a global failpoint

# Regression test: `BACKUP` takes the destination by writing its own UUID into the `.lock` file with a
# conditional write. That write can report a failure after it committed - an S3 `PutObject` with
# `If-None-Match` can time out on the client after the server applied it, and the retry then fails the
# precondition against the object the first attempt wrote. The lock then holds this attempt's own
# contents, which used to be reported as "A concurrent backup writing to the same destination detected"
# even though nothing else was writing there.

# The destination is a `File(...)` backup: its lock write is an `O_EXCL` create, which is a real
# conditional create just like the object-storage one, so the recovery below is active there. Backends
# that write the lock in rewrite mode (`Disk`, `Memory`) keep reporting the destination as taken - see
# `05055_backup_lock_file_non_atomic_backend`.

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

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT backup_fail_lock_file_write_after_commit"

# The lock file is written, and only then does the write report a failure. The destination is
# uncontended, so the backup must go through instead of failing with BACKUP_ALREADY_EXISTS.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $backup SETTINGS id='$backup_id'" | grep -o "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT backup_fail_lock_file_write_after_commit"

# The backup is usable, and the lock is gone: restoring from it must read all three rows.
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.t AS ${CLICKHOUSE_DATABASE}.t_restored FROM $backup" | grep -o "RESTORED"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM ${CLICKHOUSE_DATABASE}.t_restored"

# A second backup to the same destination must still be rejected: the destination is taken.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $backup SETTINGS id='${backup_id}_again'" 2>&1 | grep -o "BACKUP_ALREADY_EXISTS" | head -n1

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t;
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_restored;
"
