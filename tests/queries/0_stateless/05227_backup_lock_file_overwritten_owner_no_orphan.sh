#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: enables global failpoints

# On a backend without conditional create (`Disk(...)`), the lock write runs in rewrite mode, so a backup
# can write its lock over the lock of a backup that got to the destination first. That first backup has
# lost its lock the moment it was written over, and it fails at its next lock check whatever the lock
# holds by then. A backup that hits a late error after such a write must therefore not leave the lock in
# place: it would fence a destination nobody owns any more, against every later backup, until somebody
# removes it by hand. It takes back the lock that carries its contents instead.
#
# Backup B passes the "does not exist yet" check and pauses right before writing its lock. Backup A then
# writes its lock, verifies it and pauses. B resumes: its lock write commits over A's lock and reports a
# failure. B is rejected and removes the lock. A resumes and fails, because its lock is gone. A third
# backup C then finds a free destination and completes.

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

function wait_status()
{
    local operation_id="$1"
    local timeout=60
    local start=$EPOCHSECONDS
    while true; do
        local current_status
        current_status=$(${CLICKHOUSE_CLIENT} --query "SELECT status FROM system.backups WHERE id='${operation_id}'")
        if [ "${current_status}" != "CREATING_BACKUP" ]; then
            echo "${current_status}"
            break
        fi
        if ((EPOCHSECONDS-start > timeout )); then
            echo "Timeout while waiting for operation ${operation_id} to finish. The current status is ${current_status}."
            exit 1
        fi
        sleep 0.1
    done
}

# B: checks that the destination is free, then pauses before writing its lock. The pause fires once, so
# only B stops there.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT backup_pause_before_lock_file_creation"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $backup SETTINGS id='${backup_id}_b' ASYNC FORMAT Null"
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT backup_pause_before_lock_file_creation PAUSE"

# A: writes its lock, reads it back and pauses right after. The pause fires once, so only A stops there.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT backup_pause_after_lock_file_creation"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $backup SETTINGS id='${backup_id}_a' ASYNC FORMAT Null"
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT backup_pause_after_lock_file_creation PAUSE"

# B resumes: its lock write commits over A's lock, and then reports a failure. The lock holds B's
# contents, but the backend cannot prove B created it rather than overwrote it, so B is rejected -- and
# it removes the lock on its way out, because A cannot use it any more either.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT backup_fail_lock_file_write_after_commit"
${CLICKHOUSE_CLIENT} --query "SYSTEM NOTIFY FAILPOINT backup_pause_before_lock_file_creation"
wait_status "${backup_id}_b"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT backup_fail_lock_file_write_after_commit"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT backup_pause_before_lock_file_creation"
${CLICKHOUSE_CLIENT} --query "SELECT error LIKE '%BACKUP_ALREADY_EXISTS%' FROM system.backups WHERE id='${backup_id}_b'"

# A resumes and fails at its next lock check: its lock is gone.
${CLICKHOUSE_CLIENT} --query "SYSTEM NOTIFY FAILPOINT backup_pause_after_lock_file_creation"
wait_status "${backup_id}_a"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT backup_pause_after_lock_file_creation"
${CLICKHOUSE_CLIENT} --query "SELECT error LIKE '%FAILED_TO_SYNC_BACKUP_OR_RESTORE%' FROM system.backups WHERE id='${backup_id}_a'"

# C: nothing fences the destination any more, so a fresh backup completes.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $backup SETTINGS id='${backup_id}_c'" | grep -o "BACKUP_CREATED"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.t AS ${CLICKHOUSE_DATABASE}.t_restored FROM $backup" | grep -o "RESTORED"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM ${CLICKHOUSE_DATABASE}.t_restored"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t;
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_restored;
"
