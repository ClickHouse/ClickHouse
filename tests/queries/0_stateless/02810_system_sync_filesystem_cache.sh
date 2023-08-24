#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-s3-storage, no-random-settings

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -nm --query """
DROP TABLE IF EXISTS test;

CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(type = cache, max_size = '100Ki', path = ${CLICKHOUSE_TEST_UNIQUE_NAME}, delayed_cleanup_interval_ms = 10000000, disk = s3_disk), min_bytes_for_wide_part = 10485760;

INSERT INTO test SELECT 1, 'test';
"""

query_id=$RANDOM

$CLICKHOUSE_CLIENT --query_id "$query_id" --query "SELECT * FROM test FORMAT Null SETTINGS enable_filesystem_cache_log = 1"

${CLICKHOUSE_CLIENT} -q "system flush logs"

key=$($CLICKHOUSE_CLIENT -nm --query """
SELECT key FROM system.filesystem_cache_log WHERE query_id = '$query_id' ORDER BY size DESC LIMIT 1;
""")

offset=$($CLICKHOUSE_CLIENT -nm --query """
SELECT offset FROM system.filesystem_cache_log WHERE query_id = '$query_id' ORDER BY size DESC LIMIT 1;
""")

path=$($CLICKHOUSE_CLIENT -nm --query """
SELECT cache_path FROM system.filesystem_cache WHERE key = '$key' AND file_segment_range_begin = $offset;
""")

rm $path

$CLICKHOUSE_CLIENT  --query "SELECT * FROM test FORMAT Null SETTINGS enable_filesystem_cache_log = 1" 2>&1 | grep -F -e "No such file or directory" > /dev/null && echo "ok" || echo "fail"

CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=fatal/g')

$CLICKHOUSE_CLIENT --query "SYSTEM SYNC FILESYSTEM CACHE" 2>&1 | grep -q "$key" && echo 'ok' || echo 'fail'

$CLICKHOUSE_CLIENT --query "SELECT * FROM test FORMAT Null"

key=$($CLICKHOUSE_CLIENT -nm --query """
SELECT key FROM system.filesystem_cache_log WHERE query_id = '$query_id' ORDER BY size DESC LIMIT 1;
""")

offset=$($CLICKHOUSE_CLIENT -nm --query """
SELECT offset FROM system.filesystem_cache_log WHERE query_id = '$query_id' ORDER BY size DESC LIMIT 1;
""")

path=$($CLICKHOUSE_CLIENT -nm --query """
SELECT cache_path FROM system.filesystem_cache WHERE key = '$key' AND file_segment_range_begin = $offset;
""")

echo -n 'fff' > $path

#cat $path

$CLICKHOUSE_CLIENT --query "SYSTEM SYNC FILESYSTEM CACHE" 2>&1 | grep -q "$key" && echo 'ok' || echo 'fail'

$CLICKHOUSE_CLIENT --query "SELECT * FROM test FORMAT Null"

$CLICKHOUSE_CLIENT --query "SYSTEM SYNC FILESYSTEM CACHE"
