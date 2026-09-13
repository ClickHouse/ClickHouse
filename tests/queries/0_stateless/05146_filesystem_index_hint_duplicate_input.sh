#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"
echo -n 'hello' > "${CLICKHOUSE_USER_FILES_UNIQUE}/a.txt"
echo -n 'world' > "${CLICKHOUSE_USER_FILES_UNIQUE}/b.txt"

TEST_REL="${CLICKHOUSE_TEST_UNIQUE_NAME}"

# `indexHint` over a cheap column, next to an ordinary conjunct over the same column: the split
# filter carries that column as two inputs, while the block holds it once.
$CLICKHOUSE_CLIENT --query "
    SELECT name FROM filesystem('${TEST_REL}') WHERE indexHint(name = 'a.txt') AND name = 'a.txt'
"

# Two hints over the same column: the split filter carries it twice. The pruning of this table
# function is exact, so the hint does filter here - `indexHint` promises that the condition itself is
# not evaluated, not that no data is skipped.
$CLICKHOUSE_CLIENT --query "
    SELECT name FROM filesystem('${TEST_REL}') WHERE indexHint(name = 'a.txt') AND indexHint(name = 'a.txt') ORDER BY name
"

$CLICKHOUSE_CLIENT --query "
    SELECT name FROM filesystem('${TEST_REL}') WHERE name = 'a.txt'
"

$CLICKHOUSE_CLIENT --query "
    SELECT name FROM filesystem('${TEST_REL}') WHERE indexHint(name = 'a.txt') AND name = 'a.txt' SETTINGS enable_analyzer = 0
"

rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"
