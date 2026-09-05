#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# A premature S3 connection close (stream EOF before the requested range is finished) must be
# healed transparently: `ReadBufferFromS3` reconnects and resumes from the current offset instead
# of reporting EOF, which used to blow up the filesystem cache layer with a LOGICAL_ERROR
# ("Having zero bytes, but range is not finished").
# The failpoint cuts every delivered buffer in half and closes the stream, so the read below
# only completes if the recovery path works repeatedly.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

url="http://localhost:11111/test/04417_premature_eof_${CLICKHOUSE_DATABASE}.bin"

# The failpoint is process-global; always disable it on exit so a timeout or interrupt
# cannot leak it into later S3/cache reads on the shared stateless server.
cleanup() { $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT s3_read_buffer_force_premature_eof" 2>/dev/null; }
trap cleanup EXIT

# Uncompressed object (RawBLOB) read through the filesystem cache: a compressed column would
# hide a torn read behind the decompressor's own checks; RawBLOB exposes the raw bytes.
$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION s3('${url}', 'clickhouse', 'clickhouse', 'RawBLOB')
SELECT randomString(1000000) SETTINGS s3_truncate_on_insert = 1"

# Reference read without the failpoint and without the cache.
reference_hash=$($CLICKHOUSE_CLIENT -q "
SELECT cityHash64(c)
FROM s3('${url}', 'clickhouse', 'clickhouse', 'RawBLOB', 'c String')
SETTINGS enable_filesystem_cache = 0")

$CLICKHOUSE_CLIENT -q "SYSTEM CLEAR FILESYSTEM CACHE 'cache_for_readbigat'"

retries_before=$($CLICKHOUSE_CLIENT -q "
SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ReadBufferFromS3PrematureEofRetries'")

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT s3_read_buffer_force_premature_eof"

# Every stream is now cut prematurely; the query must still succeed with intact data.
healed=$($CLICKHOUSE_CLIENT -q "
SELECT length(c), cityHash64(c)
FROM s3('${url}', 'clickhouse', 'clickhouse', 'RawBLOB', 'c String')
SETTINGS filesystem_cache_name = 'cache_for_readbigat', enable_filesystem_cache = 1, max_download_threads = 1")

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT s3_read_buffer_force_premature_eof"

retries_after=$($CLICKHOUSE_CLIENT -q "
SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ReadBufferFromS3PrematureEofRetries'")

healed_length=$(echo "$healed" | cut -f1)
healed_hash=$(echo "$healed" | cut -f2)

echo "$healed_length"
[ "$healed_hash" == "$reference_hash" ] && echo "data intact" || echo "DATA MISMATCH: $healed_hash != $reference_hash"
[ "${retries_after:-0}" -gt "${retries_before:-0}" ] && echo "premature eof healed" || echo "FAILPOINT DID NOT FIRE"
