#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A syntax error at an action lists what the action expects in the bare form as in the parenthesized one.
for query in \
    "EXPLAIN TEXT SELECT 1 PAGE x" \
    "EXPLAIN TEXT (SELECT 1) PAGE x" \
    "EXPLAIN TEXT SELECT 1 ONELINE," \
    "EXPLAIN TEXT (SELECT 1) ONELINE,"
do
    $CLICKHOUSE_CLIENT --query "$query" 2>&1 | grep -o 'failed at position [0-9]* ([^)]*)\|unsigned integer\|EXPLAIN TEXT action'
done
