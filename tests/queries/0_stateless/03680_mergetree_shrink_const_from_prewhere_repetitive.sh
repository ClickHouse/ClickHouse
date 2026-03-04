#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# This test covers #88605 and #90695 - different occurences of the same issue - calling shrinkToFit on a shared column.
# Due to flaky nature of the problem, the test repeats same queries multiple times in order the trigger a simultanuous
# attempt to shrink at several exec threads.
# This test complements 03680_mergetree_shrink_const_from_prewhere.sql
#
# #88605 (const_node_1): Here we have condition with a constant "materialize(255)", for which convertToFullColumnIfConst() will return underlying column w/o copying,
# and later shrinkToFit() will be called from multiple threads on this column, and leads to UB
# #90695 (const_node_2): The combination of "materialize()" with "and()" and "toNullable()" creates nested column and a chain, where double-free happened at shrinkToFit()

setup() {
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS const_node_1;
        DROP TABLE IF EXISTS const_node_2;
        CREATE TABLE const_node_1 (v Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple();
        SYSTEM STOP MERGES const_node_1;
        INSERT INTO const_node_1 VALUES (1);
        INSERT INTO const_node_1 VALUES (2);
        INSERT INTO const_node_1 VALUES (3);
        DROP TABLE IF EXISTS const_node_2;
        CREATE TABLE const_node_2 (x Int16) ENGINE = MergeTree PARTITION BY (x) ORDER BY x;
        INSERT INTO const_node_2 VALUES (1), (2), (1), (3);
    "
}

run_queries() {
    # During 30 seconds we gonna hammer server with these SELECT queries. Before the fix, it'd crash with high probability. Not crashing is the expected success.
    local TIMELIMIT=$((SECONDS+30))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "
            SELECT v FROM const_node_1 PREWHERE and(materialize(255), *) ORDER BY v FORMAT NULL;
            SELECT median(3) IGNORE NULLS FROM const_node_2 PREWHERE and(materialize(toNullable(materialize(1))), not(materialize(100) = *)) FORMAT NULL;
        "
    done
}

cleanup() {
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS const_node_1;
        DROP TABLE IF EXISTS const_node_2;
    "
}

setup
run_queries
cleanup
