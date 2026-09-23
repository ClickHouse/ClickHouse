#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-distributed-cache

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Query level setting `filesystem_cache_boundary_alignment` must override
# `boundary_alignment` of the cache configuration also for disk read.
ALIGNMENT=$((20 * 1024 * 1024))

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS test;
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree()
ORDER BY a
SETTINGS disk = disk(type = cache,
                     max_size = '1Gi',
                     max_file_segment_size = '40Mi',
                     boundary_alignment = '1Mi',
                     cache_on_write_operations = 0,
                     path = '$CLICKHOUSE_TEST_UNIQUE_NAME',
                     name = '$CLICKHOUSE_TEST_UNIQUE_NAME',
                     disk = 's3_disk');

INSERT INTO test SELECT number, randomString(100) FROM numbers(500000);

SYSTEM DROP FILESYSTEM CACHE '$CLICKHOUSE_TEST_UNIQUE_NAME';

SET read_through_distributed_cache = 0;
SET filesystem_cache_boundary_alignment = $ALIGNMENT;

-- Read a granule from the middle of the table, so that the file segments
-- of the big column are neither at the beginning nor at the end of the file.
SELECT * FROM test WHERE a = 250000 FORMAT Null;
"

# File segments must start at a boundary of the requested alignment
# (and there must be a file segment which does not start at the beginning of the file,
# otherwise the check above is trivial).
$CLICKHOUSE_CLIENT -m -q "
SELECT count() FROM system.filesystem_cache
WHERE cache_name = '$CLICKHOUSE_TEST_UNIQUE_NAME'
AND file_segment_range_begin % $ALIGNMENT != 0;

SELECT count() > 0 FROM system.filesystem_cache
WHERE cache_name = '$CLICKHOUSE_TEST_UNIQUE_NAME'
AND file_segment_range_begin > 0;
"

$CLICKHOUSE_CLIENT -q "DROP TABLE test"
