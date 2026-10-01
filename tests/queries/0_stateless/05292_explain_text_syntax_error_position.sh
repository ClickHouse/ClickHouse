#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for query in \
    "EXPLAIN TEXT SELECT x FROM t WHERE x = ORDER BY x" \
    "EXPLAIN TEXT (SELECT x FROM t WHERE x = ORDER BY x) ONELINE" \
    "EXPLAIN TEXT SELECT x FROM t WHERE x = ORDER BY x ONELINE" \
    "EXPLAIN TEXT SELECT x FROM t garbage garbage" \
    "EXPLAIN TEXT (SELECT x FROM t garbage garbage) ONELINE"
do
    $CLICKHOUSE_CLIENT --query "$query" 2>&1 | grep -o 'failed at position [0-9]* ([^)]*)'
done
