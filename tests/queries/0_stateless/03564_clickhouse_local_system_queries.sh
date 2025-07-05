#!/usr/bin/env bash

# Test that clickhouse-local properly handles SYSTEM queries that are not supported
# These queries should throw UNSUPPORTED_METHOD errors instead of LOGICAL_ERROR

set -e

# Use the built clickhouse binary with local subcommand
CLICKHOUSE_LOCAL=${CLICKHOUSE_LOCAL:-"./build/programs/clickhouse local"}

# Test cases that should fail with UNSUPPORTED_METHOD in clickhouse-local
test_cases=(
    "SYSTEM RELOAD CONFIG"
    "SYSTEM STOP LISTEN HTTP"
    "SYSTEM START LISTEN HTTP"
    "SYSTEM STOP LISTEN TCP"
    "SYSTEM START LISTEN TCP"
)

echo "Testing clickhouse-local SYSTEM queries that should fail with UNSUPPORTED_METHOD:"

for query in "${test_cases[@]}"; do
    echo "Testing: $query"
    
    # Run clickhouse-local and capture the error
    error_output=$($CLICKHOUSE_LOCAL --query "$query" 2>&1 || true)
    
    # Check that the error contains UNSUPPORTED_METHOD
    if echo "$error_output" | grep -q "UNSUPPORTED_METHOD"; then
        echo "✓ PASS: $query failed with UNSUPPORTED_METHOD"
    else
        echo "✗ FAIL: $query did not fail with UNSUPPORTED_METHOD"
        echo "Error output: $error_output"
        exit 1
    fi
    
    # Check that the error message mentions clickhouse-local
    if echo "$error_output" | grep -q "clickhouse-local"; then
        echo "✓ PASS: Error message mentions clickhouse-local"
    else
        echo "✗ FAIL: Error message does not mention clickhouse-local"
        echo "Error output: $error_output"
        exit 1
    fi
done

echo ""
echo "Testing clickhouse-local SYSTEM queries that should work:"

# Test cases that should work in clickhouse-local
supported_queries=(
    "SYSTEM DROP DNS CACHE"
    "SYSTEM DROP MARK CACHE"
    "SYSTEM DROP UNCOMPRESSED CACHE"
    "SYSTEM DROP QUERY CACHE"
    "SYSTEM DROP SCHEMA CACHE"
    "SYSTEM DROP FORMAT SCHEMA CACHE"
)

for query in "${supported_queries[@]}"; do
    echo "Testing: $query"
    
    # Run clickhouse-local and check that it succeeds
    if $CLICKHOUSE_LOCAL --query "$query" >/dev/null 2>&1; then
        echo "✓ PASS: $query succeeded"
    else
        echo "✗ FAIL: $query failed unexpectedly"
        exit 1
    fi
done

echo ""
echo "All tests passed!"