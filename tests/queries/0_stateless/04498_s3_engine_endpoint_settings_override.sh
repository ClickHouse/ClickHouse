#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs s3

# When a setting is configured both for a specific S3 endpoint and in the general <s3> section, the
# endpoint-specific value should win for a table using that endpoint. Here the endpoint sets a small
# max_single_part_upload_size (10000), so an insert into an S3 engine table should upload in several
# parts. Before the fix the general value was used instead and the insert went as a single upload.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Unique object path per test database so parallel runs do not collide.
url="http://localhost:11111/test/04498_s3_engine_endpoint_settings_override/${CLICKHOUSE_DATABASE}/data.native"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_04498 SYNC"
$CLICKHOUSE_CLIENT --query "CREATE TABLE t_04498 (s String) ENGINE = S3('$url', 'Native')"

# Random (incompressible) strings keep the data above the 10000-byte threshold to force multipart.
$CLICKHOUSE_CLIENT --query "
INSERT INTO t_04498 SELECT randomString(100) FROM numbers(500)
SETTINGS s3_check_objects_after_upload = 0, s3_truncate_on_insert = 1
"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT --query "
SELECT
    ProfileEvents['S3CreateMultipartUpload'] >= 1,
    ProfileEvents['S3UploadPart'] >= 1,
    ProfileEvents['S3CompleteMultipartUpload'] >= 1
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query LIKE '%t_04498%'
    AND query LIKE '%INSERT%'
    AND query NOT LIKE '%system.query_log%'
ORDER BY query_start_time DESC
LIMIT 1
"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_04498 SYNC"
