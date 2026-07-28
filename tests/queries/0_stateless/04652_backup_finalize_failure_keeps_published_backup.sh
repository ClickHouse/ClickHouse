#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: the archive arms need minizip, which is not built in the fast test
# no-parallel: enables a global failpoint

# A BACKUP that fails after its backup has already been published at the destination must not have
# that backup deleted by the failure cleanup: the published backup is complete and usable, and any
# incremental chained onto it would otherwise become unrestorable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

backups_root=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.disks WHERE name='backups'" 2>/dev/null)
if [ -z "${backups_root}" ]; then
    echo "backups disk is not configured, skipping test"
    exit 0
fi

pub_dir="${CLICKHOUSE_TEST_UNIQUE_NAME}_pub"
inc_dir="${CLICKHOUSE_TEST_UNIQUE_NAME}_inc"
pub_zip="${CLICKHOUSE_TEST_UNIQUE_NAME}_pub.zip"
early_zip="${CLICKHOUSE_TEST_UNIQUE_NAME}_early.zip"
mid_zip="${CLICKHOUSE_TEST_UNIQUE_NAME}_mid.zip"
clean_dir="${CLICKHOUSE_TEST_UNIQUE_NAME}_clean"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t;
CREATE TABLE ${CLICKHOUSE_DATABASE}.t (x Int32) ENGINE = MergeTree ORDER BY x;
-- Without this a background merge may replace the part that the base backup holds, which would make
-- the incremental below self-contained and its restore would no longer depend on the base at all.
SYSTEM STOP MERGES ${CLICKHOUSE_DATABASE}.t;
INSERT INTO ${CLICKHOUSE_DATABASE}.t SELECT number FROM numbers(10);
"

# ---------------------------------------------------------------- bug arm, directory backup

echo "bug_dir_error"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT backup_fail_before_removing_lock_file"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO Disk('backups', '$pub_dir')" 2>&1 | grep -m1 -o "FAULT_INJECTED"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT backup_fail_before_removing_lock_file"

echo "bug_dir_metadata_kept"
test -f "${backups_root}/${pub_dir}/.backup" && echo 1 || echo 0

echo "bug_dir_restored"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.t AS ${CLICKHOUSE_DATABASE}.t_pub FROM Disk('backups', '$pub_dir')" | cut -f2
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM ${CLICKHOUSE_DATABASE}.t_pub"

# ------------------------------------------------- chain victim: an incremental chained onto it

echo "chain_victim_created"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.t SELECT number FROM numbers(10, 5)"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO Disk('backups', '$inc_dir') SETTINGS base_backup=Disk('backups', '$pub_dir')" | cut -f2

# The restore below only exercises the chain if the incremental really reuses data from the base.
echo "chain_victim_uses_base"
grep -q "<use_base>true</use_base>" "${backups_root}/${inc_dir}/.backup" && echo 1 || echo 0

echo "chain_victim_restored"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.t AS ${CLICKHOUSE_DATABASE}.t_inc FROM Disk('backups', '$inc_dir')" | cut -f2
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM ${CLICKHOUSE_DATABASE}.t_inc"

# ------------------------------------------------------------------ bug arm, archive backup

echo "bug_archive_error"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT backup_fail_before_removing_lock_file"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO Disk('backups', '$pub_zip')" 2>&1 | grep -m1 -o "FAULT_INJECTED"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT backup_fail_before_removing_lock_file"

echo "bug_archive_kept"
test -f "${backups_root}/${pub_zip}" && echo 1 || echo 0

echo "bug_archive_restored"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.t AS ${CLICKHOUSE_DATABASE}.t_zip FROM Disk('backups', '$pub_zip')" | cut -f2
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM ${CLICKHOUSE_DATABASE}.t_zip"

# Keeping the published backup means the lock file is left behind, because removing it is what
# failed. That is deliberate: the destination is still refused for a new backup, and it is refused
# by the published-backup check rather than by the stray lock.

echo "bug_archive_lock_kept"
test -f "${backups_root}/${pub_zip}.lock" && echo 1 || echo 0

echo "bug_archive_destination_refused"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO Disk('backups', '$pub_zip')" 2>&1 | grep -m1 -oE "is being written already|already exists"

# --------- control_early, archive: a failure BEFORE publication must still clean up (over-arming)

echo "control_early_error"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT backup_fail_before_writing_metadata"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO Disk('backups', '$early_zip')" 2>&1 | grep -m1 -o "FAULT_INJECTED"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT backup_fail_before_writing_metadata"

echo "control_early_archive_removed"
test -f "${backups_root}/${early_zip}" && echo 1 || echo 0

echo "control_early_destination_reusable"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO Disk('backups', '$early_zip')" | cut -f2

# --- control_mid, archive: a failure after `.backup` is written but BEFORE the archive is
# --- finalized is still UNPUBLISHED (the archive is not readable yet), so it must be cleaned up.
# --- This is what distinguishes arming after closeArchive(true) from arming before it.

echo "control_mid_error"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT backup_fail_while_finalizing_archive"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO Disk('backups', '$mid_zip')" 2>&1 | grep -m1 -o "FAULT_INJECTED"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT backup_fail_while_finalizing_archive"

echo "control_mid_archive_removed"
test -f "${backups_root}/${mid_zip}" && echo 1 || echo 0

echo "control_mid_destination_reusable"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO Disk('backups', '$mid_zip')" | cut -f2

# ------------------------------------------------------------------------------ control_clean

echo "control_clean"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO Disk('backups', '$clean_dir')" | cut -f2
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.t AS ${CLICKHOUSE_DATABASE}.t_clean FROM Disk('backups', '$clean_dir')" | cut -f2
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM ${CLICKHOUSE_DATABASE}.t_clean"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t;
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_pub;
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_inc;
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_zip;
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_clean;
"

rm -rf "${backups_root:?}/${pub_dir}" "${backups_root:?}/${inc_dir}" "${backups_root:?}/${clean_dir}" 2>/dev/null || true
# The archive lock is a sibling object next to the archive, so it needs removing explicitly. Only the
# published archive keeps its lock: the arms that fail before publication are still cleaned up.
rm -f "${backups_root:?}/${pub_zip}" "${backups_root:?}/${early_zip}" "${backups_root:?}/${mid_zip}" \
      "${backups_root:?}/${pub_zip}.lock" 2>/dev/null || true
