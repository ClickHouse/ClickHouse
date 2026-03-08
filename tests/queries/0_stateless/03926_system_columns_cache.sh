#!/usr/bin/env bash
# Test system.columns_cache table and SYSTEM commands
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Server-based tests for system.columns_cache table

$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

$CLICKHOUSE_CLIENT -q "DESC TABLE system.columns_cache"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_system_cache_test"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE t_system_cache_test (
    id UInt64,
    name String,
    value Float64
) ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0
"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_system_cache_test SELECT number, toString(number), number * 1.5 FROM numbers(10000)"

$CLICKHOUSE_CLIENT -q "
SELECT count(*), sum(id), sum(value)
FROM t_system_cache_test
SETTINGS use_columns_cache = 1,
         enable_writes_to_columns_cache = 1,
         enable_reads_from_columns_cache = 1
"

$CLICKHOUSE_CLIENT -q "
SELECT
    column,
    row_end - row_begin as row_count,
    rows,
    bytes > 0 as has_bytes
FROM system.columns_cache
WHERE (database = '' OR database = currentDatabase())
  AND part LIKE '%t_system_cache_test%'
ORDER BY column, row_begin
FORMAT Null
"

$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

$CLICKHOUSE_CLIENT -q "
SELECT count(*) as cache_entries
FROM system.columns_cache
WHERE (database = '' OR database = currentDatabase())
  AND part LIKE '%t_system_cache_test%'
"

$CLICKHOUSE_CLIENT -q "
SELECT count(*)
FROM t_system_cache_test
SETTINGS use_columns_cache = 1,
         enable_writes_to_columns_cache = 1
"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_system_cache_test"

$CLICKHOUSE_CLIENT -q "SELECT 'System table and SYSTEM commands test passed'"

# Test cache metrics in isolation using clickhouse-local
$CLICKHOUSE_LOCAL -q "
CREATE TABLE t_local_cache (id UInt64, value UInt64) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_local_cache SELECT number, number * 7 FROM numbers(1000);
SELECT sum(value) FROM t_local_cache SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1;
SELECT sum(value) FROM t_local_cache SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1;
SELECT event, value > 0 AS has_value FROM system.events WHERE event LIKE 'ColumnsCache%' ORDER BY event;
"
