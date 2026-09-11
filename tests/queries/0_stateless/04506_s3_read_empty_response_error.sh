#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# Tag no-fasttest: requires S3
# Tag no-parallel: toggles a global failpoint
#
# When a response delivers no data while the requested range is not finished, no progress is
# possible (resuming from the same offset would return the same empty response), so the read
# must fail loudly with CANNOT_READ_ALL_DATA - not report a silent EOF that surfaces later as
# a LOGICAL_ERROR ("Having zero bytes, but range is not finished") in the filesystem cache layer.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

url="http://localhost:11111/test/04506_empty_response_${CLICKHOUSE_DATABASE}.bin"

# The failpoint is process-global; always disable it on exit so a timeout or interrupt
# cannot leak it into later S3 reads on the shared stateless server.
cleanup() { $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT s3_read_buffer_force_empty_response" 2>/dev/null; }
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION s3('${url}', 'clickhouse', 'clickhouse', 'RawBLOB')
SELECT randomString(100000) SETTINGS s3_truncate_on_insert = 1"

$CLICKHOUSE_CLIENT -q "SYSTEM CLEAR FILESYSTEM CACHE 'cache_for_readbigat'"
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT s3_read_buffer_force_empty_response"

# The cached read requests an explicit range, so the empty response cannot be a legitimate EOF.
$CLICKHOUSE_CLIENT -q "
SELECT length(c)
FROM s3('${url}', 'clickhouse', 'clickhouse', 'RawBLOB', 'c String')
SETTINGS filesystem_cache_name = 'cache_for_readbigat', enable_filesystem_cache = 1, max_download_threads = 1" 2>&1 \
    | grep -o -m1 -E "CANNOT_READ_ALL_DATA|LOGICAL_ERROR" | head -n1
