#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Should finish in reasonable time (milliseconds).
# In previous versions this query led to exponential backtracking.

echo 'SELECT '$(perl -e 'print "CAST(" x 100')'a b c'$(perl -e 'print ")" x 100') | ${CLICKHOUSE_LOCAL} --max_parser_depth 10000 2>&1 | grep -cF 'Syntax error'
