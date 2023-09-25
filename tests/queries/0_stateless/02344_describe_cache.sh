#!/usr/bin/env bash
# Tags: no-fasttest

# set -x

disk_name="02344_describe_cache_test"

$CLICKHOUSE_CLIENT -nm --query """
DROP TABLE IF EXISTS test;
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$disk_name', type = cache, max_size = '100Ki', path = '$disk_name', disk = s3disk);
"""

$CLICKHOUSE_CLIENT -nm --query """
SELECT count() FROM system.disks WHERE name = '$disk_name'
"""

$CLICKHOUSE_CLIENT --query "DESCRIBE FILESYSTEM CACHE '${disk_name}'"
