#!/usr/bin/env bash
# Tags: long, no-parallel, no-ordinary-database
# Test is too heavy, avoid parallel run in Flaky Check

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS src";
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS dst";
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS mv";
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS tmp";
$CLICKHOUSE_CLIENT --query "CREATE TABLE src (n Int8, m Int8, CONSTRAINT c CHECK xxHash32(n+m) % 8 != 0) ENGINE=MergeTree ORDER BY n PARTITION BY 0 < n SETTINGS old_parts_lifetime=0";
$CLICKHOUSE_CLIENT --query "CREATE TABLE dst (nm Int16, CONSTRAINT c CHECK xxHash32(nm) % 8 != 0) ENGINE=MergeTree ORDER BY nm SETTINGS old_parts_lifetime=0";
$CLICKHOUSE_CLIENT --query "CREATE MATERIALIZED VIEW mv TO dst (nm Int16) AS SELECT n*m AS nm FROM src";

$CLICKHOUSE_CLIENT --query "CREATE TABLE tmp (x UInt8, nm Int16) ENGINE=MergeTree ORDER BY (x, nm) SETTINGS old_parts_lifetime=0"

$CLICKHOUSE_CLIENT --query "INSERT INTO src VALUES (0, 0)"

# some transactions will fail due to constraint
function thread_insert_commit()
{
    set -e
    for i in {1..100}; do
        $CLICKHOUSE_CLIENT --multiquery --query "
        BEGIN TRANSACTION;
        INSERT INTO src VALUES /* ($i, $1) */ ($i, $1);
        SELECT throwIf((SELECT sum(nm) FROM mv) != $(($i * $1))) FORMAT Null;
        INSERT INTO src VALUES /* (-$i, $1) */ (-$i, $1);
        COMMIT;" 2>&1| grep -Fv "is violated at row" | grep -Fv "Transaction is not in RUNNING state" | grep -F "Received from " ||:
    done
}

function thread_insert_rollback()
{
    set -e
    for _ in {1..100}; do
        $CLICKHOUSE_CLIENT --multiquery --query "
        BEGIN TRANSACTION;
        INSERT INTO src VALUES /* (42, $1) */ (42, $1);
        SELECT throwIf((SELECT count() FROM src WHERE n=42 AND m=$1) != 1) FORMAT Null;
        ROLLBACK;"
    done
}

# make merges more aggressive
function thread_optimize()
{
    set -e
    while true; do
        optimize_query="OPTIMIZE TABLE src"
        partition_id=$(( RANDOM % 2 ))
        if (( RANDOM % 2 )); then
            optimize_query="OPTIMIZE TABLE dst"
            partition_id="all"
        fi
        if (( RANDOM % 2 )); then
            optimize_query="$optimize_query PARTITION ID '$partition_id'"
        fi
        if (( RANDOM % 2 )); then
            optimize_query="$optimize_query FINAL"
        fi
        action="COMMIT"
        if (( RANDOM % 4 )); then
            action="ROLLBACK"
        fi

        $CLICKHOUSE_CLIENT --multiquery --query "
        BEGIN TRANSACTION;
        $optimize_query;
        $action;
        " 2>&1| grep -Fv "already exists, but it will be deleted soon" | grep -F "Received from " ||:
        sleep 0.$RANDOM;
    done
}

function thread_select()
{
    set -e
    while true; do
        $CLICKHOUSE_CLIENT --multiquery --query "
        BEGIN TRANSACTION;
        SELECT throwIf((SELECT (sum(n), count() % 2) FROM src) != (0, 1)) FORMAT Null;
        SELECT throwIf((SELECT (sum(nm), count() % 2) FROM mv) != (0, 1)) FORMAT Null;
        SELECT throwIf((SELECT (sum(nm), count() % 2) FROM dst) != (0, 1)) FORMAT Null;
        SELECT throwIf((SELECT arraySort(groupArray(nm)) FROM mv) != (SELECT arraySort(groupArray(nm)) FROM dst)) FORMAT Null;
        SELECT throwIf((SELECT arraySort(groupArray(nm)) FROM mv) != (SELECT arraySort(groupArray(n*m)) FROM src)) FORMAT Null;
        COMMIT;"
    done
}

function thread_select_insert()
{
    set -e
    while true; do
        $CLICKHOUSE_CLIENT --multiquery --query "
        BEGIN TRANSACTION;
        SELECT throwIf((SELECT count() FROM tmp) != 0) FORMAT Null;
        INSERT INTO tmp SELECT 1, n*m FROM src;
        INSERT INTO tmp SELECT 2, nm FROM mv;
        INSERT INTO tmp SELECT 3, nm FROM dst;
        INSERT INTO tmp SELECT 4, (*,).1 FROM (SELECT n*m FROM src UNION ALL SELECT nm FROM mv UNION ALL SELECT nm FROM dst);
        SELECT throwIf((SELECT countDistinct(x) FROM tmp) != 4) FORMAT Null;

        -- now check that all results are the same
        SELECT throwIf(1 != (SELECT countDistinct(arr) FROM (SELECT x, arraySort(groupArray(nm)) AS arr FROM tmp WHERE x!=4 GROUP BY x))) FORMAT Null;
        SELECT throwIf((SELECT count(), sum(nm) FROM tmp WHERE x=4) != (SELECT count(), sum(nm) FROM tmp WHERE x!=4)) FORMAT Null;
        ROLLBACK;"
    done
}

thread_insert_commit 1 & PID_1=$!
thread_insert_commit 2 & PID_2=$!
thread_insert_rollback 3 & PID_3=$!

thread_optimize & PID_4=$!
thread_select & PID_5=$!
thread_select_insert & PID_6=$!
sleep 0.$RANDOM;
thread_select & PID_7=$!
thread_select_insert & PID_8=$!

wait $PID_1 && wait $PID_2 && wait $PID_3
kill -TERM $PID_4
kill -TERM $PID_5
kill -TERM $PID_6
kill -TERM $PID_7
kill -TERM $PID_8
wait
wait_for_queries_to_finish

$CLICKHOUSE_CLIENT --multiquery --query "
BEGIN TRANSACTION;
SELECT count(), sum(n), sum(m=1), sum(m=2), sum(m=3) FROM src;
SELECT count(), sum(nm) FROM mv";

$CLICKHOUSE_CLIENT --query "SELECT count(), sum(n), sum(m=1), sum(m=2), sum(m=3) FROM src"
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(nm) FROM mv"

$CLICKHOUSE_CLIENT --query "DROP TABLE src";
$CLICKHOUSE_CLIENT --query "DROP TABLE dst";
$CLICKHOUSE_CLIENT --query "DROP TABLE mv";
$CLICKHOUSE_CLIENT --query "DROP TABLE tmp";
