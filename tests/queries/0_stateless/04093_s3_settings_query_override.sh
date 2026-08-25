#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs s3

# Verify that query-level S3 settings properly override global <s3> endpoint
# configuration when using the s3() table function. This exercises the fromAST
# code path in Configuration.cpp where user/profile/query-level settings must
# take priority over global <s3> endpoint configuration.
#
# The test depends on tests/config/config.d/s3_settings_override.xml which
# configures a matching global <s3> endpoint with max_single_part_upload_size = 100Mi.
# Without the fix, the global endpoint config would override the query-level
# setting, resulting in a single-part upload instead of multipart.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Use query-level SETTINGS with a small value to force multipart upload.
# If query-level settings are properly re-applied after endpoint config,
# the small value will be used and we'll see multipart upload in ProfileEvents.
# Use a deterministic 100-byte string so the upload size is predictable and
# stable under random test settings; a small row count keeps the test fast.
# Disable `s3_check_objects_after_upload` because the size verification has
# been observed to be flaky against the local S3 mock under the flaky check
# (random settings combined with multipart upload), and this test only needs
# to verify that the multipart upload path is taken, not upload integrity.
$CLICKHOUSE_CLIENT --query "
INSERT INTO TABLE FUNCTION s3('http://localhost:11111/test/04093_s3_settings_query_override.csv', 'CSV', 's String')
SELECT repeat('x', 100) FROM numbers(500)
SETTINGS s3_max_single_part_upload_size = 10000, s3_truncate_on_insert = 1, s3_check_objects_after_upload = 0
"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

# With s3_max_single_part_upload_size = 10000 at query level, the data should
# be uploaded via multipart (CreateMultipartUpload + UploadPart + CompleteMultipartUpload).
# If the global endpoint config (100Mi) had incorrectly taken priority, it would be a single PutObject.
$CLICKHOUSE_CLIENT --query "
SELECT
    ProfileEvents['S3CreateMultipartUpload'] >= 1 AS has_multipart_create,
    ProfileEvents['S3UploadPart'] >= 1 AS has_upload_parts,
    ProfileEvents['S3CompleteMultipartUpload'] >= 1 AS has_multipart_complete
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query LIKE '%04093_s3_settings_query_override.csv%'
    AND query NOT LIKE '%system.query_log%'
ORDER BY query_start_time DESC
LIMIT 1
"
