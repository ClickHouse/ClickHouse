#!/usr/bin/env bash
# Round-trip test for backup metadata: the `.backup` manifest is written by
# `writeBackupMetadata` and read back by `readBackupMetadata`. A mis-parse of the
# manifest (file names, sizes, checksums, base-backup dedup fields) would either
# fail the RESTORE or restore wrong data, so restoring and comparing the data is a
# direct check of the read path. Covers a full backup and an incremental backup on
# top of it (base-backup dedup fields).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

full_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_full
incr_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_incr

full_backup="Disk('backups', '$full_id')"
incr_backup="Disk('backups', '$incr_id')"

# Deterministic data (no now()/rand()), several inserts so the backup lists many files across parts.
${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS expected_full;
DROP TABLE IF EXISTS src_from_full;
DROP TABLE IF EXISTS src_from_incr;

CREATE TABLE src (k UInt64, s String, t DateTime) ENGINE = MergeTree ORDER BY k;
INSERT INTO src SELECT number, repeat('x', number % 17), toDateTime('2020-01-01 00:00:00') + number FROM numbers(0, 1000);
INSERT INTO src SELECT number, repeat('y', number % 13), toDateTime('2020-01-01 00:00:00') + number FROM numbers(1000, 500);

-- Snapshot of the table as it is captured by the full backup.
CREATE TABLE expected_full ENGINE = MergeTree ORDER BY k AS SELECT * FROM src;
"

# Full backup.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.src TO $full_backup SETTINGS id='$full_id'" | grep -o "BACKUP_CREATED"

# Add more data, then an incremental backup on top of the full one.
${CLICKHOUSE_CLIENT} --query "INSERT INTO src SELECT number, repeat('z', number % 11), toDateTime('2020-01-01 00:00:00') + number FROM numbers(2000, 300)"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.src TO $incr_backup SETTINGS id='$incr_id', base_backup=$full_backup" | grep -o "BACKUP_CREATED"

# Restore each backup into a separate table (this is what exercises readBackupMetadata).
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.src AS ${CLICKHOUSE_DATABASE}.src_from_full FROM $full_backup" | grep -o "RESTORED"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.src AS ${CLICKHOUSE_DATABASE}.src_from_incr FROM $incr_backup" | grep -o "RESTORED"

# Compare restored data against the expected state. `label: OK` iff the sets are equal.
compare() {
    ${CLICKHOUSE_CLIENT} --query "
    SELECT '$3: ' || if(
        (SELECT count() FROM (SELECT * FROM $1 EXCEPT SELECT * FROM $2)) = 0
        AND (SELECT count() FROM (SELECT * FROM $2 EXCEPT SELECT * FROM $1)) = 0
        AND (SELECT count() FROM $1) = (SELECT count() FROM $2),
        'OK', 'MISMATCH')"
}

# The full backup captured expected_full; the incremental captured the live src (with the extra rows).
compare "${CLICKHOUSE_DATABASE}.src_from_full" "${CLICKHOUSE_DATABASE}.expected_full" "full disk"
compare "${CLICKHOUSE_DATABASE}.src_from_incr" "${CLICKHOUSE_DATABASE}.src" "incremental"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS expected_full;
DROP TABLE IF EXISTS src_from_full;
DROP TABLE IF EXISTS src_from_incr;
"
