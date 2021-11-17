#!/usr/bin/env bash
# Tags: long

# shellcheck disable=SC2015

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS src";
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS dst";
$CLICKHOUSE_CLIENT --query "CREATE TABLE src (n UInt64, type UInt8) ENGINE=MergeTree ORDER BY type SETTINGS old_parts_lifetime=0";
$CLICKHOUSE_CLIENT --query "CREATE TABLE dst (n UInt64, type UInt8) ENGINE=MergeTree ORDER BY type SETTINGS old_parts_lifetime=0";

function thread_insert()
{
    set -e
    trap "exit 0" INT
    while true; do
        action="ROLLBACK"
        if (( RANDOM % 2 )); then
            action="COMMIT"
        fi
        val=$RANDOM
        $CLICKHOUSE_CLIENT --multiquery --query "
        BEGIN TRANSACTION;
        INSERT INTO src VALUES ($val, 1);
        INSERT INTO src VALUES ($val, 2);
        COMMIT;"
        sleep 0.$RANDOM;
    done
}


# NOTE
# ALTER PARTITION query stops merges,
# but serialization error is still possible if some merge was assigned (and committed) between BEGIN and ALTER.
function thread_partition_src_to_dst()
{
    set -e
    count=0
    sum=0
    for i in {1..20}; do
        out=$(
        $CLICKHOUSE_CLIENT --multiquery --query "
        BEGIN TRANSACTION;
        INSERT INTO src VALUES ($i, 3);
        INSERT INTO dst SELECT * FROM src;
        ALTER TABLE src DROP PARTITION ID 'all';
        SELECT throwIf((SELECT (count(), sum(n)) FROM merge(currentDatabase(), '') WHERE type=3) != ($count + 1, $sum + $i)) FORMAT Null;
        COMMIT;" 2>&1) ||:

        echo "$out" | grep -Fv "SERIALIZATION_ERROR" | grep -F "Received from " && $CLICKHOUSE_CLIENT -q "SELECT _table, type, arraySort(groupArray(n)) FROM merge(currentDatabase(), '') GROUP BY _table, type ORDER BY _table, type" ||:
        echo "$out" | grep -Fa "SERIALIZATION_ERROR" >/dev/null || count=$((count+1)) && sum=$((sum+i))
    done
}

function thread_partition_dst_to_src()
{
    set -e
    for i in {1..20}; do
        action="ROLLBACK"
        if (( i % 2 )); then
            action="COMMIT"
        fi
        $CLICKHOUSE_CLIENT --multiquery --query "
        SYSTEM STOP MERGES dst;
        BEGIN TRANSACTION;
        INSERT INTO dst VALUES ($i, 4);
        INSERT INTO src SELECT * FROM dst;
        ALTER TABLE dst DROP PARTITION ID 'all';
        SYSTEM START MERGES dst;
        SELECT throwIf((SELECT (count(), sum(n)) FROM merge(currentDatabase(), '') WHERE type=4) != (toUInt8($i/2 + 1), (select sum(number) from numbers(1, $i) where number % 2 or number=$i))) FORMAT Null;
        $action;" || $CLICKHOUSE_CLIENT -q "SELECT _table, type, arraySort(groupArray(n)) FROM merge(currentDatabase(), '') GROUP BY _table, type ORDER BY _table, type"
    done
}

function thread_select()
{
    set -e
    trap "exit 0" INT
    while true; do
        $CLICKHOUSE_CLIENT --multiquery --query "
        BEGIN TRANSACTION;
        -- no duplicates
        SELECT type, throwIf(count(n) != countDistinct(n)) FROM src GROUP BY type FORMAT Null;
        SELECT type, throwIf(count(n) != countDistinct(n)) FROM dst GROUP BY type FORMAT Null;
        -- rows inserted by thread_insert moved together
        SELECT _table, throwIf(arraySort(groupArrayIf(n, type=1)) != arraySort(groupArrayIf(n, type=2))) FROM merge(currentDatabase(), '') GROUP BY _table FORMAT Null;
        COMMIT;" || $CLICKHOUSE_CLIENT -q "SELECT _table, type, arraySort(groupArray(n)) FROM merge(currentDatabase(), '') GROUP BY _table, type ORDER BY _table, type"
    done
}

thread_insert & PID_1=$!
thread_select & PID_2=$!

thread_partition_src_to_dst & PID_3=$!
thread_partition_dst_to_src & PID_4=$!
wait $PID_3 && wait $PID_4

kill -INT $PID_1
kill -INT $PID_2
wait

$CLICKHOUSE_CLIENT -q "SELECT type, count(n) = countDistinct(n) FROM merge(currentDatabase(), '') GROUP BY type ORDER BY type"
$CLICKHOUSE_CLIENT -q "SELECT DISTINCT arraySort(groupArrayIf(n, type=1)) = arraySort(groupArrayIf(n, type=2)) FROM merge(currentDatabase(), '') GROUP BY _table ORDER BY _table"
$CLICKHOUSE_CLIENT -q "SELECT count(n), sum(n) FROM merge(currentDatabase(), '') WHERE type=4"


$CLICKHOUSE_CLIENT --query "DROP TABLE src";
$CLICKHOUSE_CLIENT --query "DROP TABLE dst";
