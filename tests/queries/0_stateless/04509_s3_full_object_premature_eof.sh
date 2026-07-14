#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# Tag no-fasttest: requires S3
# Tag no-parallel: toggles a global failpoint
#
# Companion to 04417, for the full-object read path. `S3ObjectStorage::readObject` reads a whole
# object with `read_until_position = 0` but a known `file_size`, so a premature connection close
# must be healed using `file_size` as the expected upper bound - otherwise the mid-stream close is
# treated as a clean EOF and the read is silently truncated. This read goes straight through
# `ReadBufferFromS3` without the filesystem cache, so it exercises the `file_size` fallback.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

url="http://localhost:11111/test/04509_full_object_premature_eof_${CLICKHOUSE_DATABASE}.bin"

# The failpoint is process-global; always disable it on exit so a timeout or interrupt
# cannot leak it into later S3 reads on the shared stateless server.
cleanup() { $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT s3_read_buffer_force_premature_eof" 2>/dev/null; }
trap cleanup EXIT

# Uncompressed object (RawBLOB) read without the filesystem cache: a compressed column would hide
# a torn read behind the decompressor's own checks; RawBLOB exposes the raw bytes.
$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION s3('${url}', 'clickhouse', 'clickhouse', 'RawBLOB')
SELECT randomString(1000000) SETTINGS s3_truncate_on_insert = 1"

# Reference read without the failpoint.
reference_hash=$($CLICKHOUSE_CLIENT -q "
SELECT cityHash64(c)
FROM s3('${url}', 'clickhouse', 'clickhouse', 'RawBLOB', 'c String')
SETTINGS enable_filesystem_cache = 0")

retries_before=$($CLICKHOUSE_CLIENT -q "
SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ReadBufferFromS3PrematureEofRetries'")

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT s3_read_buffer_force_premature_eof"

# Every stream is now cut prematurely; a full-object read (read_until_position = 0, known
# file_size) must still succeed with intact data by resuming from the current offset.
healed=$($CLICKHOUSE_CLIENT -q "
SELECT length(c), cityHash64(c)
FROM s3('${url}', 'clickhouse', 'clickhouse', 'RawBLOB', 'c String')
SETTINGS enable_filesystem_cache = 0, remote_filesystem_read_method = 'read'")

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT s3_read_buffer_force_premature_eof"

retries_after=$($CLICKHOUSE_CLIENT -q "
SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ReadBufferFromS3PrematureEofRetries'")

healed_length=$(echo "$healed" | cut -f1)
healed_hash=$(echo "$healed" | cut -f2)

echo "$healed_length"
[ "$healed_hash" == "$reference_hash" ] && echo "data intact" || echo "DATA MISMATCH: $healed_hash != $reference_hash"
[ "${retries_after:-0}" -gt "${retries_before:-0}" ] && echo "premature eof healed" || echo "FAILPOINT DID NOT FIRE"
