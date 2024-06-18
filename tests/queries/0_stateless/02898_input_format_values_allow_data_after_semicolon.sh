#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "client no multiquery"
$CLICKHOUSE_CLIENT -q "insert into function null() values (1); -- { foo }" |& grep -F -o "Cannot read data after semicolon (and input_format_values_allow_data_after_semicolon=0)"
echo "client multiquery"
$CLICKHOUSE_CLIENT -n -q "insert into function null() values (1); -- { foo }"

echo "local no multiquery"
$CLICKHOUSE_LOCAL -q "insert into function null() values (1); -- { foo }" |& grep -F -o "Cannot read data after semicolon (and input_format_values_allow_data_after_semicolon=0)"
echo "local multiquery"
$CLICKHOUSE_LOCAL -n -q "insert into function null() values (1); -- { foo }"
