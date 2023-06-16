#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -nm --query "
DROP TABLE IF EXISTS test_s3;

CREATE TABLE test_s3 (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = 's3_disk', min_bytes_for_wide_part = 0;

INSERT INTO test_s3 SELECT number, number FROM numbers_mt(1e7);
"
query="SELECT a, b FROM test_s3"
query_id=$(${CLICKHOUSE_CLIENT} --query "select queryID() from ($query) limit 1" 2>&1)
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS"
${CLICKHOUSE_CLIENT} -nm --query "
WITH
    ProfileEvents['ReadBufferFromS3ResetSessions'] AS reset,
    ProfileEvents['ReadBufferFromS3PreservedSessions'] AS preserved
SELECT preserved > reset
FROM system.query_log
WHERE type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query_id='$query_id';
"
