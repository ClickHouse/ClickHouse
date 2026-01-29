#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

# Test that MergeTree introspection functions check table grants correctly.
# These functions should require SELECT permission on the source table.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_grants_mt"
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS test_user_03812"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE test_grants_mt (key Int, value Int, INDEX idx_value value TYPE minmax GRANULARITY 1)
ENGINE = MergeTree() ORDER BY key
"

$CLICKHOUSE_CLIENT -q "INSERT INTO test_grants_mt SELECT number, number * 10 FROM numbers(1000)"

# Create user without any grants
$CLICKHOUSE_CLIENT -q "CREATE USER test_user_03812"

# Test mergeTreeAnalyzeIndexes - should fail without SELECT grant
echo "=== mergeTreeAnalyzeIndexes without grant ==="
$CLICKHOUSE_CLIENT --user test_user_03812 -q "
SELECT count() FROM mergeTreeAnalyzeIndexes(currentDatabase(), test_grants_mt)
" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

# Test mergeTreeIndex - should fail without SELECT grant
echo "=== mergeTreeIndex without grant ==="
$CLICKHOUSE_CLIENT --user test_user_03812 -q "
SELECT count() FROM mergeTreeIndex(currentDatabase(), test_grants_mt)
" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

# Grant SELECT permission
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.test_grants_mt TO test_user_03812"

# Test mergeTreeAnalyzeIndexes - should work with SELECT grant
echo "=== mergeTreeAnalyzeIndexes with grant ==="
$CLICKHOUSE_CLIENT --user test_user_03812 -q "
SELECT count() > 0 FROM mergeTreeAnalyzeIndexes(currentDatabase(), test_grants_mt)
"

# Test mergeTreeIndex - should work with SELECT grant
echo "=== mergeTreeIndex with grant ==="
$CLICKHOUSE_CLIENT --user test_user_03812 -q "
SELECT count() > 0 FROM mergeTreeIndex(currentDatabase(), test_grants_mt)
"

# Test mergeTreeAnalyzeIndexesUUID
TABLE_UUID=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'test_grants_mt'")

# Revoke grants for UUID test
$CLICKHOUSE_CLIENT -q "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.test_grants_mt FROM test_user_03812"

# Test mergeTreeAnalyzeIndexesUUID - should fail without SELECT grant
echo "=== mergeTreeAnalyzeIndexesUUID without grant ==="
$CLICKHOUSE_CLIENT --user test_user_03812 -q "
SELECT count() FROM mergeTreeAnalyzeIndexesUUID('$TABLE_UUID')
" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

# Grant SELECT permission again
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.test_grants_mt TO test_user_03812"

# Test mergeTreeAnalyzeIndexesUUID - should work with SELECT grant
echo "=== mergeTreeAnalyzeIndexesUUID with grant ==="
$CLICKHOUSE_CLIENT --user test_user_03812 -q "
SELECT count() > 0 FROM mergeTreeAnalyzeIndexesUUID('$TABLE_UUID')
"

# Test mergeTreeAnalyzeIndexes with column-level grant only
$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS test_user_03812_col"
$CLICKHOUSE_CLIENT -q "CREATE USER test_user_03812_col"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(key) ON ${CLICKHOUSE_DATABASE}.test_grants_mt TO test_user_03812_col"

# mergeTreeAnalyzeIndexes checks table-level SELECT, should fail with column-only grant
echo "=== mergeTreeAnalyzeIndexes with column-only grant ==="
$CLICKHOUSE_CLIENT --user test_user_03812_col -q "
SELECT count() FROM mergeTreeAnalyzeIndexes(currentDatabase(), test_grants_mt)
" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

# Cleanup
$CLICKHOUSE_CLIENT -q "DROP TABLE test_grants_mt"
$CLICKHOUSE_CLIENT -q "DROP USER test_user_03812"
$CLICKHOUSE_CLIENT -q "DROP USER test_user_03812_col"
