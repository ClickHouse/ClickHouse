#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_LOCAL --path "${CLICKHOUSE_TMP}" "CREATE FUNCTION f AS x -> x + 1; SELECT f(1);"
$CLICKHOUSE_LOCAL --path "${CLICKHOUSE_TMP}" "SELECT f(2); DROP FUNCTION f;"
