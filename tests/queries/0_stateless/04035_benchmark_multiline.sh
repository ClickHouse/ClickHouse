#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "SELECT
1" | ${CLICKHOUSE_BENCHMARK} --iterations 1 --concurrency 1 --multiline \
    2>&1 | grep -q "Queries executed: 1" && echo "OK: Single multiline query"

# Test multiple multiline queries separated by semicolon
echo "SELECT
1;
SELECT
2;
SELECT
3" | ${CLICKHOUSE_BENCHMARK} --iterations 1 --concurrency 1 --multiline \
    2>&1 | grep -q "Loaded 3 queries" && echo "OK: Multiple multiline queries"

echo "SELECT
1" | ${CLICKHOUSE_BENCHMARK} --iterations 1 --concurrency 1 -m \
    2>&1 | grep -q "Queries executed: 1" && echo "OK: Short option -m"

# Test that semicolons inside comments don't split queries
echo "SELECT
-- comment with ; semicolon
1;
SELECT /* block comment; with semicolon */ 2" | ${CLICKHOUSE_BENCHMARK} --iterations 1 --concurrency 1 --multiline \
    2>&1 | grep -q "Loaded 2 queries" && echo "OK: Semicolons in comments ignored"

# Test default mode still works (one query per line)
echo "SELECT 1
SELECT 2" | ${CLICKHOUSE_BENCHMARK} --iterations 1 --concurrency 1 \
    2>&1 | grep -q "Loaded 2 queries" && echo "OK: Default mode (line-by-line)"

echo "All multiline tests passed"
