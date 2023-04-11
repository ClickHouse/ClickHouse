#!/usr/bin/env bash

# Tags: no-parallel

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "SELECT plu(1, 1) SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['plus'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT uniqExac(1, 1) SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['uniqExact'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "DROP FUNCTION IF EXISTS test_user_defined_function;"
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION test_user_defined_function AS x -> x + 1;"
$CLICKHOUSE_CLIENT -q "SELECT test_user_defined_functio(1) SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['test_user_defined_function'\]" &>/dev/null;
$CLICKHOUSE_CLIENT -q "DROP FUNCTION test_user_defined_function";

$CLICKHOUSE_CLIENT -q "WITH (x -> x + 1) AS lambda_function SELECT lambda_functio(1) SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['lambda_function'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT row_numbe() OVER (PARTITION BY 1) SETTINGS allow_experimental_analyzer = 1;" 2>&1 \
    | grep "Maybe you meant: \['row_number'\]" &>/dev/null;

$CLICKHOUSE_CLIENT -q "SELECT 1";
