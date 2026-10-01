#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An error inside a parenthesized `EXPLAIN TEXT` source is reported at the offending token of the
# statement, also when a later statement follows, and when the source is not closed before it.
for query in \
    "SELECT 1; EXPLAIN TEXT (SELECT x FROM t WHERE x = ORDER BY x) ONELINE; SELECT 3" \
    "EXPLAIN TEXT (SELECT 1; SELECT 2) ONELINE; SELECT 3"
do
    $CLICKHOUSE_CLIENT --multiquery --query "$query" 2>&1 | grep -o 'failed at position [0-9]* ([^)]*)'
done
