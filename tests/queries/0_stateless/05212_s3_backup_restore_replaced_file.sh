#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BACKUP_PATH="backups/${CLICKHOUSE_DATABASE}/05212_replaced"
BACKUP_URL="http://localhost:11111/test/${BACKUP_PATH}"

# One table on the local disk and one on an S3 disk: a restore of the first reads every file of the
# backup through a buffer, a restore of the second copies it from S3 to S3 natively. Their contents
# differ, so that the backup does not deduplicate the data file of one onto the data file of the other,
# and the parts are wide, fully stored (not packed) and have plain file names, so that the data file of
# the only column is `n.bin`.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE on_local (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, replace_long_file_name_to_hash = 0"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE on_s3 (n UInt64) ENGINE = MergeTree ORDER BY n SETTINGS disk = 's3_disk', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, replace_long_file_name_to_hash = 0"
${CLICKHOUSE_CLIENT} -q "INSERT INTO on_local SELECT number FROM numbers(100)"
${CLICKHOUSE_CLIENT} -q "INSERT INTO on_s3 SELECT number * 2 FROM numbers(100)"
${CLICKHOUSE_CLIENT} -q "BACKUP TABLE on_local, TABLE on_s3 TO S3(s3_conn, '${BACKUP_PATH}')" > /dev/null

# Replace one data file of each table inside the backup with a longer object. The backup metadata
# still records the original size, and a restore copies exactly that many bytes of every file, so the
# replacement would be restored as its first bytes if the reader did not compare the object with the
# metadata before reading it.
for table in on_local on_s3
do
    ${CLICKHOUSE_CLIENT} --s3_truncate_on_insert 1 -q "INSERT INTO FUNCTION s3('${BACKUP_URL}/data/${CLICKHOUSE_DATABASE}/${table}/all_1_1_0/n.bin', 'test', 'testtest', 'TSV') SELECT repeat('x', 100000)"
done

# The buffered restore, and the native S3-to-S3 copy of the restore: both are refused before a byte
# is read or copied, whether the reads are pinned to a generation or not.
for pinned in 1 0
do
    ${CLICKHOUSE_CLIENT} --s3_validate_etag_on_read ${pinned} -q "RESTORE TABLE on_local AS restored_local FROM S3(s3_conn, '${BACKUP_PATH}')" 2>&1 \
        | grep -oF "S3_OBJECT_CHANGED_DURING_READ" | head -n 1
    ${CLICKHOUSE_CLIENT} --s3_validate_etag_on_read ${pinned} -q "RESTORE TABLE on_s3 AS restored_s3 FROM S3(s3_conn, '${BACKUP_PATH}') SETTINGS allow_s3_native_copy = true" 2>&1 \
        | grep -oF "S3_OBJECT_CHANGED_DURING_READ" | head -n 1
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS restored_local"
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS restored_s3"
done

# A backup nobody touched restores through both paths: the check refuses a replaced file, not every file.
${CLICKHOUSE_CLIENT} -q "BACKUP TABLE on_local, TABLE on_s3 TO S3(s3_conn, '${BACKUP_PATH}_intact')" > /dev/null
${CLICKHOUSE_CLIENT} -q "RESTORE TABLE on_local AS restored_local FROM S3(s3_conn, '${BACKUP_PATH}_intact')" > /dev/null
${CLICKHOUSE_CLIENT} -q "RESTORE TABLE on_s3 AS restored_s3 FROM S3(s3_conn, '${BACKUP_PATH}_intact') SETTINGS allow_s3_native_copy = true" > /dev/null
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(n) FROM restored_local"
${CLICKHOUSE_CLIENT} -q "SELECT count(), sum(n) FROM restored_s3"
