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

$CLICKHOUSE_CLIENT --query """
SYSTEM SYNC FILESYSTEM CACHE '$disk_name' ON CLUSTER 'test_shard_localhost';
"""

$CLICKHOUSE_CLIENT --query """
SYSTEM DROP FILESYSTEM CACHE '$disk_name' ON CLUSTER 'test_shard_localhost';
"""

$CLICKHOUSE_CLIENT --query """
DROP TABLE IF EXISTS test;
"""
