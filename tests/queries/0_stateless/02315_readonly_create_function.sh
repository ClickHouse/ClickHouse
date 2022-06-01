#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --readonly 1 --query "CREATE FUNCTION test_function AS (x) -> x + 1;" 2>&1 | grep -c -F 'Code: 164'
