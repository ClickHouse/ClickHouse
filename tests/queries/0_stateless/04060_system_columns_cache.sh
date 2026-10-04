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

# The populating query above must have written the table's columns to the cache.
# Filter by the table name, not the data part name (`part` is `all_1_1_0`, which
# never contains the table name).
$CLICKHOUSE_CLIENT -q "
SELECT count() > 0
FROM system.columns_cache
WHERE database = currentDatabase()
  AND table = 't_system_cache_test'
"

$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

$CLICKHOUSE_CLIENT -q "
SELECT count(*) as cache_entries
FROM system.columns_cache
WHERE database = currentDatabase()
  AND table = 't_system_cache_test'
"

$CLICKHOUSE_CLIENT -q "
SELECT count(*)
FROM t_system_cache_test
SETTINGS use_columns_cache = 1,
         enable_writes_to_columns_cache = 1
"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_system_cache_test"

# `SystemColumnsCacheSource::generate` is called repeatedly and has to resume where the previous
# chunk stopped, so the whole cache is reported no matter how many entries it holds. Warm several
# entries and read the table with `max_block_size = 1`, which forces one entry per chunk: a source
# that only ever returned its first chunk would report a single row here, and one that restarted
# from the beginning would never finish.

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_system_cache_pagination"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE t_system_cache_pagination (
    id UInt64,
    a UInt64,
    b UInt64,
    c String
) ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1000
"

# Four parts times four columns give plenty of independent cache entries.
for part in 0 1 2 3
do
    $CLICKHOUSE_CLIENT -q "
    INSERT INTO t_system_cache_pagination
    SELECT number + ${part} * 1000, number, number * 2, toString(number) FROM numbers(1000)
    "
done

$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

$CLICKHOUSE_CLIENT -q "
SELECT count(), sum(a), sum(b), sum(length(c))
FROM t_system_cache_pagination
SETTINGS use_columns_cache = 1,
         enable_writes_to_columns_cache = 1,
         enable_reads_from_columns_cache = 1
"

# Identify the table by UUID: it is the only key that survives in a cache entry.
table_uuid=$($CLICKHOUSE_CLIENT -q "
SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_system_cache_pagination'
")

entries=$($CLICKHOUSE_CLIENT -q "
SELECT count() FROM system.columns_cache WHERE table_uuid = '$table_uuid'
")

paginated_entries=$($CLICKHOUSE_CLIENT -q "
SELECT count() FROM system.columns_cache WHERE table_uuid = '$table_uuid' SETTINGS max_block_size = 1
")

# The rows themselves must be the same, not only how many there are: an off-by-one in the
# pagination cursor would keep the count while dropping or repeating an entry.
checksum_query="
SELECT sum(sipHash64(part, column, row_begin, row_end, rows, bytes))
FROM system.columns_cache
WHERE table_uuid = '$table_uuid'
"

checksum=$($CLICKHOUSE_CLIENT -q "$checksum_query")
paginated_checksum=$($CLICKHOUSE_CLIENT -q "$checksum_query SETTINGS max_block_size = 1")

echo "more than one entry warmed: $([ "$entries" -gt 1 ] && echo 1 || echo 0)"
echo "row count with max_block_size = 1 matches: $([ "$entries" = "$paginated_entries" ] && echo 1 || echo 0)"
echo "row contents with max_block_size = 1 match: $([ "$checksum" = "$paginated_checksum" ] && echo 1 || echo 0)"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_system_cache_pagination"

$CLICKHOUSE_CLIENT -q "SELECT 'System table and SYSTEM commands test passed'"
