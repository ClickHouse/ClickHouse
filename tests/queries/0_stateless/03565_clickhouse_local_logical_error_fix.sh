#!/bin/bash

# Test that the old LOGICAL_ERROR behavior has been fixed in clickhouse-local
# These queries used to throw LOGICAL_ERROR but should now throw UNSUPPORTED_METHOD

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "Testing clickhouse-local SYSTEM queries that should throw UNSUPPORTED_METHOD:"

# Test SYSTEM RELOAD CONFIG
echo "Testing: SYSTEM RELOAD CONFIG"
if $CLICKHOUSE_LOCAL --query "SYSTEM RELOAD CONFIG;" 2>&1 | grep -q "UNSUPPORTED_METHOD"; then
    echo "✓ PASS: SYSTEM RELOAD CONFIG failed with UNSUPPORTED_METHOD"
    if $CLICKHOUSE_LOCAL --query "SYSTEM RELOAD CONFIG;" 2>&1 | grep -q "clickhouse-local"; then
        echo "✓ PASS: Error message mentions clickhouse-local"
    else
        echo "✗ FAIL: Error message does not mention clickhouse-local"
        exit 1
    fi
else
    echo "✗ FAIL: SYSTEM RELOAD CONFIG did not throw UNSUPPORTED_METHOD"
    exit 1
fi

# Test SYSTEM STOP LISTEN HTTP
echo "Testing: SYSTEM STOP LISTEN HTTP"
if $CLICKHOUSE_LOCAL --query "SYSTEM STOP LISTEN HTTP;" 2>&1 | grep -q "UNSUPPORTED_METHOD"; then
    echo "✓ PASS: SYSTEM STOP LISTEN HTTP failed with UNSUPPORTED_METHOD"
    if $CLICKHOUSE_LOCAL --query "SYSTEM STOP LISTEN HTTP;" 2>&1 | grep -q "clickhouse-local"; then
        echo "✓ PASS: Error message mentions clickhouse-local"
    else
        echo "✗ FAIL: Error message does not mention clickhouse-local"
        exit 1
    fi
else
    echo "✗ FAIL: SYSTEM STOP LISTEN HTTP did not throw UNSUPPORTED_METHOD"
    exit 1
fi

# Test SYSTEM START LISTEN HTTP
echo "Testing: SYSTEM START LISTEN HTTP"
if $CLICKHOUSE_LOCAL --query "SYSTEM START LISTEN HTTP;" 2>&1 | grep -q "UNSUPPORTED_METHOD"; then
    echo "✓ PASS: SYSTEM START LISTEN HTTP failed with UNSUPPORTED_METHOD"
    if $CLICKHOUSE_LOCAL --query "SYSTEM START LISTEN HTTP;" 2>&1 | grep -q "clickhouse-local"; then
        echo "✓ PASS: Error message mentions clickhouse-local"
    else
        echo "✗ FAIL: Error message does not mention clickhouse-local"
        exit 1
    fi
else
    echo "✗ FAIL: SYSTEM START LISTEN HTTP did not throw UNSUPPORTED_METHOD"
    exit 1
fi

echo "Testing clickhouse-local SYSTEM queries that should work:"

# Test that other SYSTEM queries still work
echo "Testing: SYSTEM DROP DNS CACHE"
if $CLICKHOUSE_LOCAL --query "SYSTEM DROP DNS CACHE;" >/dev/null 2>&1; then
    echo "✓ PASS: SYSTEM DROP DNS CACHE succeeded"
else
    echo "✗ FAIL: SYSTEM DROP DNS CACHE failed"
    exit 1
fi

echo "Testing: SYSTEM DROP MARK CACHE"
if $CLICKHOUSE_LOCAL --query "SYSTEM DROP MARK CACHE;" >/dev/null 2>&1; then
    echo "✓ PASS: SYSTEM DROP MARK CACHE succeeded"
else
    echo "✗ FAIL: SYSTEM DROP MARK CACHE failed"
    exit 1
fi

echo "Testing: SYSTEM DROP UNCOMPRESSED CACHE"
if $CLICKHOUSE_LOCAL --query "SYSTEM DROP UNCOMPRESSED CACHE;" >/dev/null 2>&1; then
    echo "✓ PASS: SYSTEM DROP UNCOMPRESSED CACHE succeeded"
else
    echo "✗ FAIL: SYSTEM DROP UNCOMPRESSED CACHE failed"
    exit 1
fi

echo "Testing: SYSTEM DROP QUERY CACHE"
if $CLICKHOUSE_LOCAL --query "SYSTEM DROP QUERY CACHE;" >/dev/null 2>&1; then
    echo "✓ PASS: SYSTEM DROP QUERY CACHE succeeded"
else
    echo "✗ FAIL: SYSTEM DROP QUERY CACHE failed"
    exit 1
fi

echo "Testing: SYSTEM DROP SCHEMA CACHE"
if $CLICKHOUSE_LOCAL --query "SYSTEM DROP SCHEMA CACHE;" >/dev/null 2>&1; then
    echo "✓ PASS: SYSTEM DROP SCHEMA CACHE succeeded"
else
    echo "✗ FAIL: SYSTEM DROP SCHEMA CACHE failed"
    exit 1
fi

echo "Testing: SYSTEM DROP FORMAT SCHEMA CACHE"
if $CLICKHOUSE_LOCAL --query "SYSTEM DROP FORMAT SCHEMA CACHE;" >/dev/null 2>&1; then
    echo "✓ PASS: SYSTEM DROP FORMAT SCHEMA CACHE succeeded"
else
    echo "✗ FAIL: SYSTEM DROP FORMAT SCHEMA CACHE failed"
    exit 1
fi

echo "All tests passed!" 