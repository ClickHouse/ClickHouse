#!/usr/bin/env bash

# A JOIN with a `merge` table over many tables with different structures used to spend a cubic
# amount of work in query planning: for every source table the planner resolved every column of the
# combined structure with a separate run of the query analyzer, and every such run rebuilt the
# analysis data for all the columns of the `Merge` table. Before the fix the queries below took
# seconds in a release build (and timed out in sanitizer builds); now they take a fraction of a
# second. The sizes are deliberately moderate so that the test stays fast in debug and sanitizer
# builds after the fix.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLES=20
COLUMNS=10

for ((i = 1; i <= TABLES; ++i))
do
    columns="a UInt64"
    for ((j = 1; j <= COLUMNS; ++j))
    do
        columns="$columns, t${i}_c${j} UInt64"
    done
    echo "CREATE TABLE t${i} ($columns) ENGINE = Memory;"
    echo "INSERT INTO t${i} (a) VALUES ($i);"
done | $CLICKHOUSE_CLIENT

# The structure of the `Merge` table is the union of the structures of the source tables, so `*`
# expands to a column for every column of every source table. The left side of the join produces a
# constant, which makes the result independent of the order in which the source tables are read.
# The old analyzer cannot resolve columns of a `Merge` table that are missing from some of the
# source tables, so the analyzer is enforced.
$CLICKHOUSE_CLIENT --enable_analyzer 1 --query "
    SELECT count(), sum(sipHash64(*))
    FROM (SELECT 0 AS n FROM numbers(100)) AS t1
    PASTE JOIN merge(currentDatabase(), '') AS t2
"

$CLICKHOUSE_CLIENT --enable_analyzer 1 --query "
    SELECT count(), sum(sipHash64(*))
    FROM (SELECT 0 AS n FROM numbers(3)) AS t1
    CROSS JOIN merge(currentDatabase(), '') AS t2
"
