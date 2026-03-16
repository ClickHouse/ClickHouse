#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test that clickhouse-local has access management privileges by default
# Previously, DROP ROW POLICY IF EXISTS would fail with ACCESS_DENIED error

${CLICKHOUSE_LOCAL} -q "
    CREATE TABLE test (x UInt8) ENGINE = Memory;
    CREATE ROW POLICY pol ON test USING x > 0;
    DROP ROW POLICY pol ON test;
    DROP ROW POLICY IF EXISTS pol ON test;
    SELECT 'OK';
"
