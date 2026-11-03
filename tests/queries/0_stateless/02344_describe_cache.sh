#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

disk_name="02344_describe_cache_test"

$CLICKHOUSE_CLIENT -m --query """
DROP TABLE IF EXISTS test;
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$disk_name', type = cache, max_size = '100Ki', path = '$disk_name', disk = 's3_disk', load_metadata_asynchronously = 0);
"""

$CLICKHOUSE_CLIENT -m --query """
SELECT count() FROM system.disks WHERE name = '$disk_name'
"""

$CLICKHOUSE_CLIENT --query "DESCRIBE FILESYSTEM CACHE '${disk_name}'"
