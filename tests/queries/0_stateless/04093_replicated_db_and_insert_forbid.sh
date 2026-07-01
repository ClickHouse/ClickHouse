#!/usr/bin/env bash
# Tags: replica, zookeeper

# Test for CREATE TABLE IF NOT EXISTS ... AND INSERT restrictions with ReplicatedMergeTree
# in a Replicated database. 
# See: https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/InterpreterCreateQuery.cpp#L1820

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Create a Replicated database
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CLICKHOUSE_DATABASE}_rmt_db engine = Replicated('/clickhouse/databases/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${CLICKHOUSE_DATABASE}_rmt_db', '{shard}', '{replica}')"

# Create a ReplicatedMergeTree table
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query "CREATE TABLE ${CLICKHOUSE_DATABASE}_rmt_db.rmt_table (a UInt32, b String) ENGINE = ReplicatedMergeTree ORDER BY a"

# Try to INSERT into the existing replicated table using AND INSERT - should fail
echo "Test 1: AND INSERT on existing ReplicatedMergeTree should fail with SUPPORT_IS_DISABLED"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --query "CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}_rmt_db.rmt_table (a UInt32, b String) ENGINE = MergeTree ORDER BY a AND INSERT SELECT 1 AS a, 'test' AS b" 2>&1 | grep -cm1 "AND INSERT into an existing table with a Replicated engine is not supported"

# Try with database_replicated_allow_heavy_create=1 - should also fail for AND INSERT
echo "Test 2: AND INSERT on existing ReplicatedMergeTree fails even with database_replicated_allow_heavy_create=1"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --database_replicated_allow_heavy_create=1 --query "CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}_rmt_db.rmt_table (a UInt32, b String) ENGINE = MergeTree ORDER BY a AND INSERT SELECT 2 AS a, 'test2' AS b" 2>&1 | grep -cm1 "AND INSERT into an existing table with a Replicated engine is not supported"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${CLICKHOUSE_DATABASE}_rmt_db"

echo "All tests passed!"
