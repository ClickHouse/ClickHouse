#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo -e "a,b,c\n1,2,3" > $CLICKHOUSE_TEST_UNIQUE_NAME.csvwithnames

$CLICKHOUSE_LOCAL -q "select b from file('$CLICKHOUSE_TEST_UNIQUE_NAME.csvwithnames') settings input_format_with_names_use_header=0"

rm $CLICKHOUSE_TEST_UNIQUE_NAME.csvwithnames
