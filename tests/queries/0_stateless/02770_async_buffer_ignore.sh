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
query_id=$(${CLICKHOUSE_CLIENT} -nm --query "
SET read_through_distributed_cache=0;
SET remote_filesystem_read_method='threadpool';
select queryID() from ($query) limit 1
" 2>&1)
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
# This query seeks each column once, from offset 0, so a correct lazy ignore never
# triggers: `AsynchronousReaderIgnoredBytes` is the oracle for the prefix over-read.
# `RemoteFSSeeks` guards against it passing vacuously on the synchronous path,
# where there is no `AsynchronousBoundedReadBuffer` to exercise.
# `ReadCompressedBytes` bounds granule selection only; it counts blocks handed to
# the decompressor and so cannot see a discarded prefix.
${CLICKHOUSE_CLIENT} -m --query "
SELECT
    ProfileEvents['RemoteFSSeeks'] > 0,
    ProfileEvents['AsynchronousReaderIgnoredBytes'] = 0,
    ProfileEvents['S3ReadRequestsCount'] < 100,
    ProfileEvents['ReadCompressedBytes'] < 1000000
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND query_id='$query_id';
"
