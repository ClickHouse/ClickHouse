#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# Tag no-fasttest: requires S3
# Tag no-parallel: toggles a global failpoint
#
# Companion to 04417 and 04509, for the S3 backup reader path. `BackupReaderS3::readFile` reads a
# whole backup object (e.g. `.backup` metadata, consumed with `readStringUntilEOF`) with
# `read_until_position = 0`. It must thread the known object size into `ReadBufferFromS3` so that a
# premature connection close is healed using `file_size` as the expected upper bound - otherwise the
# mid-stream close is treated as a clean EOF and the backup metadata (or a data file) is silently
# truncated, and `RESTORE` reads a corrupt backup.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The failpoint is process-global; always disable it on exit so a timeout or interrupt
# cannot leak it into later S3 reads on the shared stateless server.
cleanup() { $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT s3_read_buffer_force_premature_eof" 2>/dev/null; }
trap cleanup EXIT

backup_name="S3(s3_conn, 'backups/${CLICKHOUSE_DATABASE}/premature_eof')"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS data;
CREATE TABLE data (key UInt64, value String) ENGINE = MergeTree ORDER BY key;
INSERT INTO data SELECT number, randomString(1000) FROM numbers(10000);
"

reference_count=$($CLICKHOUSE_CLIENT -q "SELECT count(), sum(cityHash64(key, value)) FROM data")

# Create the backup on S3 without the failpoint. `allow_s3_native_copy = false` forces the data
# files to be read back through `ReadBufferFromS3` on restore instead of a server-side copy.
$CLICKHOUSE_CLIENT --format Null -q "BACKUP TABLE data TO ${backup_name} SETTINGS allow_s3_native_copy = false"

retries_before=$($CLICKHOUSE_CLIENT -q "
SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ReadBufferFromS3PrematureEofRetries'")

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT s3_read_buffer_force_premature_eof"

# Every stream is now cut prematurely. Reading the `.backup` metadata (full object,
# read_until_position = 0, known file_size) and the data files must still succeed with intact data
# by resuming from the current offset, so the restore must round-trip the table faithfully.
$CLICKHOUSE_CLIENT --format Null -q "RESTORE TABLE data AS data_restored FROM ${backup_name} SETTINGS allow_s3_native_copy = false"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT s3_read_buffer_force_premature_eof"

retries_after=$($CLICKHOUSE_CLIENT -q "
SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ReadBufferFromS3PrematureEofRetries'")

restored_count=$($CLICKHOUSE_CLIENT -q "SELECT count(), sum(cityHash64(key, value)) FROM data_restored")

[ "$restored_count" == "$reference_count" ] && echo "data intact" || echo "DATA MISMATCH: $restored_count != $reference_count"
[ "${retries_after:-0}" -gt "${retries_before:-0}" ] && echo "premature eof healed" || echo "FAILPOINT DID NOT FIRE"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS data; DROP TABLE IF EXISTS data_restored;"
