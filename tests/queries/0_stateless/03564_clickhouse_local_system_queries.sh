#!/usr/bin/env bash

# Test that clickhouse-local properly handles SYSTEM queries that are not supported
# These queries should throw UNSUPPORTED_METHOD errors instead of LOGICAL_ERROR

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_LOCAL --query "SYSTEM RELOAD CONFIG; -- { serverError UNSUPPORTED_METHOD }"
$CLICKHOUSE_LOCAL --query "SYSTEM STOP LISTEN HTTP; -- { serverError UNSUPPORTED_METHOD }"
$CLICKHOUSE_LOCAL --query "SYSTEM START LISTEN HTTP; -- { serverError UNSUPPORTED_METHOD }"
$CLICKHOUSE_LOCAL --query "SYSTEM STOP LISTEN TCP; -- { serverError UNSUPPORTED_METHOD }"
$CLICKHOUSE_LOCAL --query "SYSTEM START LISTEN TCP; -- { serverError UNSUPPORTED_METHOD }"


$CLICKHOUSE_LOCAL --query "SYSTEM DROP DNS CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM DROP MARK CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM DROP UNCOMPRESSED CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM DROP QUERY CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM DROP SCHEMA CACHE;"
$CLICKHOUSE_LOCAL --query "SYSTEM DROP FORMAT SCHEMA CACHE;"