#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "Test 1: Check exception in local mode"
# Run query in clickhouse-local, should fail with UNSUPPORTED_METHOD
${CLICKHOUSE_LOCAL} --query "SYSTEM RELOAD CONFIG" 2>&1 | grep -F "SYSTEM RELOAD CONFIG query is not supported in clickhouse-local" && echo "OK" || echo "FAIL" 
