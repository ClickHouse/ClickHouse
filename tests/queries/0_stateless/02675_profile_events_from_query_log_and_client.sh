#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs s3

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "INSERT TO S3"
$CLICKHOUSE_CLIENT --print-profile-events --profile-events-delay-ms=-1 -nq "
INSERT INTO TABLE FUNCTION s3('http://localhost:11111/test/profile_events.csv', 'test', 'testtest', 'CSV', 'number UInt64') SELECT number FROM numbers(1000000) SETTINGS s3_max_single_part_upload_size = 10, s3_truncate_on_insert = 1;
" 2>&1 | grep -o -e '\ \[\ .*\ \]\ S3.*:\ .*\ ' | grep -v 'Microseconds' | grep -v 'S3DiskConnections' | grep -v 'S3DiskAddresses' | sort

echo "CHECK WITH query_log"
$CLICKHOUSE_CLIENT -nq "
SYSTEM FLUSH LOGS;
SELECT type,
       'S3CreateMultipartUpload', ProfileEvents['S3CreateMultipartUpload'],
       'S3UploadPart', ProfileEvents['S3UploadPart'],
       'S3CompleteMultipartUpload', ProfileEvents['S3CompleteMultipartUpload'],
       'S3PutObject', ProfileEvents['S3PutObject']
FROM system.query_log
WHERE query LIKE '%profile_events.csv%'
AND type = 'QueryFinish'
AND current_database = currentDatabase()
ORDER BY query_start_time DESC;
"

echo "CREATE"
$CLICKHOUSE_CLIENT -nq "
DROP TABLE IF EXISTS times;
CREATE TABLE times (t DateTime) ENGINE MergeTree ORDER BY t
  SETTINGS
    storage_policy='default',
    min_rows_for_wide_part = 1000000,
    min_bytes_for_wide_part = 1000000,
    ratio_of_defaults_for_sparse_serialization=1.0;
"

echo "INSERT"
$CLICKHOUSE_CLIENT --print-profile-events --profile-events-delay-ms=-1  -nq "
INSERT INTO times SELECT now() + INTERVAL 1 day SETTINGS optimize_on_insert = 0;
" 2>&1 | grep -o -e '\ \[\ .*\ \]\ FileOpen:\ .*\ '

echo "READ"
$CLICKHOUSE_CLIENT --print-profile-events --profile-events-delay-ms=-1  -nq "
SELECT '1', min(t) FROM times SETTINGS optimize_use_implicit_projections = 1;
" 2>&1 | grep -o -e '\ \[\ .*\ \]\ FileOpen:\ .*\ '

echo "INSERT and READ INSERT"
$CLICKHOUSE_CLIENT --print-profile-events --profile-events-delay-ms=-1  -nq "
INSERT INTO times SELECT now() + INTERVAL 2 day SETTINGS optimize_on_insert = 0;
SELECT '2', min(t) FROM times SETTINGS optimize_use_implicit_projections = 1;
INSERT INTO times SELECT now() + INTERVAL 3 day SETTINGS optimize_on_insert = 0;
" 2>&1 | grep -o -e '\ \[\ .*\ \]\ FileOpen:\ .*\ '

echo "DROP"
$CLICKHOUSE_CLIENT -nq "
DROP TABLE times;
"

echo "CHECK with query_log"
$CLICKHOUSE_CLIENT -nq "
SYSTEM FLUSH LOGS;
SELECT type,
       query,
       'FileOpen', ProfileEvents['FileOpen']
FROM system.query_log
WHERE current_database = currentDatabase()
AND ( query LIKE '%SELECT % FROM times%' OR query LIKE '%INSERT INTO times%' )
AND type = 'QueryFinish'
ORDER BY query_start_time_microseconds ASC, query DESC;
"
