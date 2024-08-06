#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# throw exception
$CLICKHOUSE_CLIENT -q "select 1; select 2;" 2>&1 | grep -o 'Multi-statements are not allowed'
$CLICKHOUSE_LOCAL -q "select 1; select 2;" 2>&1 | grep -o 'Multi-statements are not allowed'
# execute correctly
$CLICKHOUSE_CLIENT -n -q "select 1; select 2;"
$CLICKHOUSE_LOCAL -n -q "select 1; select 2;"

exit 0
