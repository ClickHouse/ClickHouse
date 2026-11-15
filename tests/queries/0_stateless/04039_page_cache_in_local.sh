#!/usr/bin/env bash

# Verify that the userspace page cache is initialized in clickhouse-local.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CONFIG_FILE="${CLICKHOUSE_TMP}/page_cache_config.yaml"

> "${CONFIG_FILE}" echo "
page_cache_max_size: 134217728
"

# Create a MergeTree table, insert data, read it with page cache for local disks enabled,
# and check that PageCacheBytes metric becomes non-zero.
# local_filesystem_read_method=pread is required for the page cache on local disks.
$CLICKHOUSE_LOCAL --config-file "${CONFIG_FILE}" \
    --use_page_cache_for_local_disks 1 \
    --local_filesystem_read_method pread \
    --multiquery "
CREATE TABLE test (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO test SELECT number FROM numbers(100000);
SELECT count() FROM test WHERE NOT ignore(x) FORMAT Null;
SELECT value > 0 FROM system.metrics WHERE metric = 'PageCacheBytes';
"

rm "${CONFIG_FILE}"
