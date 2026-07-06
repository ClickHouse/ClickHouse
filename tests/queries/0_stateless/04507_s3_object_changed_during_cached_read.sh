#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# Tag no-fasttest: requires S3
# Tag no-parallel: toggles a global failpoint
#
# An object overwritten during a cached read must fail with S3_OBJECT_CHANGED_DURING_READ:
# the etag captured at read setup is carried into the cache download GETs (If-Match and
# response-etag comparison in ReadBufferFromS3), so a concurrent overwrite cannot poison the
# cache or produce a torn read. Complements 04339_s3_read_etag_validation, which covers the
# non-cached path.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

url="http://localhost:11111/test/04507_etag_cached_${CLICKHOUSE_DATABASE}.csv"

# The failpoint is process-global; always disable it on exit so a timeout or interrupt
# cannot leak it into later S3 reads on the shared stateless server.
cleanup() { $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT s3_read_inject_etag_mismatch" 2>/dev/null; }
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION s3('${url}', 'clickhouse', 'clickhouse', 'CSV', 'n UInt64')
SELECT number FROM numbers(100) SETTINGS s3_truncate_on_insert = 1"

$CLICKHOUSE_CLIENT -q "SYSTEM CLEAR FILESYSTEM CACHE 'cache_for_readbigat'"

# Force every GET to report a different ETag than the one captured at read setup, simulating a
# concurrent in-place overwrite happening while the cache downloads the object.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT s3_read_inject_etag_mismatch"

$CLICKHOUSE_CLIENT -q "
SELECT count()
FROM s3('${url}', 'clickhouse', 'clickhouse', 'CSV', 'n UInt64')
SETTINGS s3_validate_etag_on_read = 1, filesystem_cache_name = 'cache_for_readbigat', enable_filesystem_cache = 1" 2>&1 \
    | grep -o -m1 -E "S3_OBJECT_CHANGED_DURING_READ|LOGICAL_ERROR|CANNOT_READ_ALL_DATA" | head -n1

# Validation off: the etag is not propagated, so the same cached read succeeds.
$CLICKHOUSE_CLIENT -q "
SELECT count()
FROM s3('${url}', 'clickhouse', 'clickhouse', 'CSV', 'n UInt64')
SETTINGS s3_validate_etag_on_read = 0, filesystem_cache_name = 'cache_for_readbigat', enable_filesystem_cache = 1"
