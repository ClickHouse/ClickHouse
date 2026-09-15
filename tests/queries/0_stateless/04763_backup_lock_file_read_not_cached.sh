#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: enables a global failpoint

# Regression for the '.lock' verification read of a backup writer being served from a read cache.
# BackupImpl::checkLockFile re-reads the lock file this process just wrote. The mmap cache is keyed
# by path + offset + length only, so after a failed attempt's lock at the same path was removed, the
# retry's read returned the previous attempt's UUID and the retry aborted with BACKUP_ALREADY_EXISTS
# even though no concurrent backup existed.
#
# use_reader_executor = 1 is LOAD-BEARING: only that read path passes the file size down to
# createReadBufferFromFileBase, which is what makes the 36-byte lock read pass the mmap threshold.
# Without it the read never uses mmap and this test would pass on the unfixed binary.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mmap_settings="local_filesystem_read_method = 'mmap', min_bytes_to_use_mmap_io = 1, use_reader_executor = 1"

base_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_base
inc_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_inc
base_backup="Disk('backups', '$base_id')"
inc_backup="Disk('backups', '$inc_id')"
retry_comment=${CLICKHOUSE_TEST_UNIQUE_NAME}_retry
retry_query_id=${CLICKHOUSE_TEST_UNIQUE_NAME}_retry_qid

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS t;
CREATE TABLE t (x Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t VALUES (1), (2), (3);
"

${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $base_backup SETTINGS id = '$base_id'" | grep -o "BACKUP_CREATED"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT backup_fail_before_writing_metadata"

# First attempt fails and its cleanup removes the lock file, leaving a mapping of it in the cache.
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $inc_backup SETTINGS id = '$inc_id', base_backup = $base_backup, $mmap_settings" 2>&1 | grep -o "FAULT_INJECTED" | head -n1

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT backup_fail_before_writing_metadata"

# The retry writes a fresh lock file at the same path with the same length. Verifying it must observe
# the file just written, not the mapping cached for the previous attempt.
${CLICKHOUSE_CLIENT} --query_id "$retry_query_id" --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $inc_backup SETTINGS id = '${inc_id}_retry', base_backup = $base_backup, $mmap_settings, log_comment = '$retry_comment'" | grep -o "BACKUP_CREATED"

# The retry must have read the lock file at all: a zero here would mean the assertion above passes
# for want of a lock check rather than because the fix works.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
SELECT ProfileEvents['BackupLockFileReads'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '$retry_comment' AND type >= 2
ORDER BY event_time_microseconds DESC LIMIT 1"

# The LOCK read specifically must have gone through the reader executor, the sole local read path
# that passes the file size down. A zero means that read was never armed for the mmap cache, so the
# assertion above would hold for want of a mapping rather than because the fix works.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} --query "
SELECT count() > 0 FROM system.text_log
WHERE query_id = '$retry_query_id' AND logger_name = 'LocalSourceReader'
  AND message LIKE 'open: file=%/.lock, size=%'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t"
