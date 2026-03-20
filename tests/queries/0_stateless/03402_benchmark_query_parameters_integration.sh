#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Create a temporary table for testing
${CLICKHOUSE_CLIENT} --query "CREATE TABLE IF NOT EXISTS ${CLICKHOUSE_DATABASE}.test_benchmark_params (
    id UInt32,
    name String,
    value Float64
) ENGINE = Memory"

# Insert test data
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${CLICKHOUSE_DATABASE}.test_benchmark_params VALUES
    (1, 'test1', 10.5),
    (2, 'test2', 20.5),
    (3, 'test3', 30.5)"

# Test SELECT with parameter
${CLICKHOUSE_BENCHMARK} --iterations 5 --concurrency 2 \
    --param_target_id 2 \
    --query "SELECT * FROM ${CLICKHOUSE_DATABASE}.test_benchmark_params WHERE id = {target_id:UInt32}" \
    2>&1 | grep -q "Queries executed: 5" && echo "OK: Parameterized SELECT"

# Test with IN clause and array parameter
${CLICKHOUSE_BENCHMARK} --iterations 3 --concurrency 1 \
    --param_ids '[1,3]' \
    --query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.test_benchmark_params WHERE id IN {ids:Array(UInt32)}" \
    2>&1 | grep -q "Queries executed: 3" && echo "OK: Parameterized IN clause"

# Test aggregation with parameter
${CLICKHOUSE_BENCHMARK} --iterations 2 --concurrency 1 \
    --param_threshold 15.0 \
    --query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.test_benchmark_params WHERE value > {threshold:Float64}" \
    2>&1 | grep -q "Queries executed: 2" && echo "OK: Parameterized aggregation"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.test_benchmark_params"

echo "Integration tests passed"
