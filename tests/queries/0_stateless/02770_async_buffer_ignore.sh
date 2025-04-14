#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-distributed-cache

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS test_s3;

CREATE TABLE test_s3 (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = 's3_disk', min_bytes_for_wide_part = 0;

INSERT INTO test_s3 SELECT number, number FROM numbers(1000000);
"
query="SELECT sum(b) FROM test_s3 WHERE a >= 100000 AND a <= 102000"
query_id=$(${CLICKHOUSE_CLIENT} --query "select queryID() from ($query) limit 1" 2>&1)
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} -m --query "
SELECT
    ProfileEvents['S3ReadRequestsCount'],
    ProfileEvents['ReadBufferFromS3Bytes'],
    ProfileEvents['ReadCompressedBytes']
FROM system.query_log
WHERE type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query_id='$query_id';
"
