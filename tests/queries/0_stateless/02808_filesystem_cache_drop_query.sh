#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-object-storage, no-random-settings

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


disk_name="${CLICKHOUSE_TEST_UNIQUE_NAME}"
$CLICKHOUSE_CLIENT -m --query """
DROP TABLE IF EXISTS test;
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$disk_name', type = cache, max_size = '100Ki', path = ${CLICKHOUSE_TEST_UNIQUE_NAME}, disk = s3_disk);

INSERT INTO test SELECT 1, 'test';
"""

query_id=$RANDOM

$CLICKHOUSE_CLIENT --query_id "$query_id" --query "SELECT * FROM test FORMAT Null SETTINGS enable_filesystem_cache_log = 1"

$CLICKHOUSE_CLIENT -m --query """
SYSTEM DROP FILESYSTEM CACHE '$disk_name' KEY kek;
""" 2>&1 | grep -q "Invalid cache key hex: kek" && echo "OK" || echo "FAIL"

${CLICKHOUSE_CLIENT} -q " system flush logs"

key=$($CLICKHOUSE_CLIENT -m --query """
SELECT key FROM system.filesystem_cache_log WHERE query_id = '$query_id' ORDER BY size DESC LIMIT 1;
""")

offset=$($CLICKHOUSE_CLIENT -m --query """
SELECT offset FROM system.filesystem_cache_log WHERE query_id = '$query_id' ORDER BY size DESC LIMIT 1;
""")

$CLICKHOUSE_CLIENT -m --query """
SELECT count() FROM system.filesystem_cache WHERE key = '$key' AND file_segment_range_begin = $offset;
"""

$CLICKHOUSE_CLIENT -m --query """
SYSTEM DROP FILESYSTEM CACHE '$disk_name' KEY $key OFFSET $offset;
"""

$CLICKHOUSE_CLIENT -m --query """
SELECT count() FROM system.filesystem_cache WHERE key = '$key' AND file_segment_range_begin = $offset;
"""

query_id=$RANDOM$RANDOM

$CLICKHOUSE_CLIENT --query_id "$query_id" --query "SELECT * FROM test FORMAT Null SETTINGS enable_filesystem_cache_log = 1"

${CLICKHOUSE_CLIENT} -q " system flush logs"

key=$($CLICKHOUSE_CLIENT -m --query """
SELECT key FROM system.filesystem_cache_log WHERE query_id = '$query_id' ORDER BY size DESC LIMIT 1;
""")

$CLICKHOUSE_CLIENT -m --query """
SELECT count() FROM system.filesystem_cache WHERE key = '$key';
"""

$CLICKHOUSE_CLIENT -m --query """
SYSTEM DROP FILESYSTEM CACHE '$disk_name' KEY $key
"""

$CLICKHOUSE_CLIENT -m --query """
SELECT count() FROM system.filesystem_cache WHERE key = '$key';
"""
