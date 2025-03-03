#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs s3

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
-- Limit S3 PUT request per second rate
SET s3_max_put_rps = 2;
SET s3_max_put_burst = 1;

CREATE TEMPORARY TABLE times (t DateTime);

-- INSERT query requires 3 PUT requests and 1/rps = 0.5 second in between, the first query is not throttled due to burst
INSERT INTO times SELECT now();
INSERT INTO TABLE FUNCTION s3('http://localhost:11111/test/request-throttler.csv', 'test', 'testtest', 'CSV', 'number UInt64') SELECT number FROM numbers(1000000) SETTINGS s3_max_single_part_upload_size = 10000, s3_truncate_on_insert = 1;
INSERT INTO times SELECT now();

SELECT max(t) - min(t) >= 1 FROM times;

SYSTEM FLUSH LOGS;
SELECT ProfileEvents['S3CreateMultipartUpload'] == 1,
       ProfileEvents['S3UploadPart'] == 1,
       ProfileEvents['S3CompleteMultipartUpload'] == 1
FROM system.query_log
WHERE query LIKE '%request-throttler.csv%'
AND type = 'QueryFinish'
AND current_database = currentDatabase()
ORDER BY query_start_time DESC
LIMIT 1;
"
