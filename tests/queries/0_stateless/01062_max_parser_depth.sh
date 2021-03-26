#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

echo 'select 1' | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}&max_parser_depth=42" -d @- 2>&1 | grep -oP "Maximum parse depth .* exceeded."
echo -
echo 'select (1+1)*(2+1)' | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}&max_parser_depth=42" -d @- 2>&1 | grep -oP "Maximum parse depth .* exceeded."
echo -
echo 'select 1' | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}&max_parser_depth=20" -d @- 2>&1 | grep -oP "Maximum parse depth .* exceeded."
