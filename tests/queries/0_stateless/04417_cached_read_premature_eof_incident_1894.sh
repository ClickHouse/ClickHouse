#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# A premature S3 connection close on a full-size object must surface as a
# retryable CANNOT_READ_ALL_DATA, not a LOGICAL_ERROR ("Having zero bytes, but range is not finished").

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

url="http://localhost:11111/test/04417_premature_eof_${CLICKHOUSE_DATABASE}.bin"

# The failpoint is process-global; always disable it on exit so a timeout or interrupt
# cannot leak it into later S3/cache reads on the shared stateless server.
cleanup() { $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT s3_read_buffer_force_premature_eof" 2>/dev/null; }
trap cleanup EXIT

# Uncompressed object (RawBLOB) read through the filesystem cache hits readFromFileSegment directly;
# a compressed column would trip the decompressor's own check first and never reach this invariant.
$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION s3('${url}', 'clickhouse', 'clickhouse', 'RawBLOB')
SELECT randomString(1000000) SETTINGS s3_truncate_on_insert = 1"

$CLICKHOUSE_CLIENT -q "SYSTEM CLEAR FILESYSTEM CACHE 'cache_for_readbigat'"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT s3_read_buffer_force_premature_eof"

# The forced stream EOF lands with offset < read_until_position on a full-size object.
$CLICKHOUSE_CLIENT -q "
SELECT length(c)
FROM s3('${url}', 'clickhouse', 'clickhouse', 'RawBLOB', 'c String')
SETTINGS filesystem_cache_name = 'cache_for_readbigat', enable_filesystem_cache = 1, max_download_threads = 1" 2>&1 \
    | grep -o -m1 -E "CANNOT_READ_ALL_DATA|LOGICAL_ERROR" | head -n1
