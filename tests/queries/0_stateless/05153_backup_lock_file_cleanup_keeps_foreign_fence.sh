#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: enables global failpoints

# On a backend without conditional create (`Disk(...)`), a backup that hits a late error after writing the
# `.lock` cannot tell whether it created that lock or wrote it over the lock of a backup that got to the
# destination first. Its cleanup must not remove the lock then: doing so would unfence the destination
# while the other backup is still writing, and a third backup could take it. The fenced destination must
# survive the failed attempt, and the backup in flight must complete.
#
# Backup A passes the "does not exist yet" check and pauses right before writing its lock. Backup B then
# takes the same destination: its lock write commits and reports a failure. B is rejected, and its lock
# must stay in place, so a third backup C is rejected too. Then A resumes and completes.

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

# A: checks that the destination is free, then pauses before writing its lock. The pause fires once, so
# only A stops there.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT backup_pause_before_lock_file_creation"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $backup SETTINGS id='${backup_id}_a' ASYNC FORMAT Null"
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT backup_pause_before_lock_file_creation PAUSE"

# B: writes the lock, which commits, and then the write reports a failure. The lock holds B's contents,
# but the backend cannot prove B created it rather than overwrote it, so B is rejected.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT backup_fail_lock_file_write_after_commit"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $backup SETTINGS id='${backup_id}_b'" 2>&1 | grep -o "BACKUP_ALREADY_EXISTS" | head -n1
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT backup_fail_lock_file_write_after_commit"

# C: the destination must still be fenced. If B had removed the lock on its way out, C would take the
# destination while A is still writing to it.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $backup SETTINGS id='${backup_id}_c'" 2>&1 | grep -o "BACKUP_ALREADY_EXISTS" | head -n1

# A resumes, writes its lock and completes: the destination is A's.
${CLICKHOUSE_CLIENT} --query "SYSTEM NOTIFY FAILPOINT backup_pause_before_lock_file_creation"
wait_status "${backup_id}_a"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT backup_pause_before_lock_file_creation"

${CLICKHOUSE_CLIENT} --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.t AS ${CLICKHOUSE_DATABASE}.t_restored FROM $backup" | grep -o "RESTORED"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM ${CLICKHOUSE_DATABASE}.t_restored"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t;
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_restored;
"
