#!/usr/bin/env bash
# Tags: no-fasttest

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

disk_name="${CLICKHOUSE_TEST_UNIQUE_NAME}_1"

$CLICKHOUSE_CLIENT -m --query """
DROP TABLE IF EXISTS test;
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$disk_name', type = cache, max_size = '100Ti', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_1', disk = s3_disk);
""" 2>&1 | grep -q "The total capacity of the disk containing cache path" && echo 'OK' || echo 'FAIL'

$CLICKHOUSE_CLIENT -m --query """
SELECT count() FROM system.disks WHERE name = '$disk_name'
"""

disk_name="${CLICKHOUSE_TEST_UNIQUE_NAME}_2"

$CLICKHOUSE_CLIENT -m --query """
DROP TABLE IF EXISTS test;
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$disk_name', type = cache, max_size = '100Ki', path = '${CLICKHOUSE_TEST_UNIQUE_NAME}_2', disk = s3_disk);
"""

$CLICKHOUSE_CLIENT -m --query """
SELECT count() FROM system.disks WHERE name = '$disk_name'
"""
