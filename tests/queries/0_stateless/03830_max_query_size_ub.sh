#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&max_query_size=18446744073709551615" -d "EXPLAIN TABLE OVERRIDE  SELECT 1,    1" |& grep -o "Exception.*" | sed 's/Exception/exception/g'
