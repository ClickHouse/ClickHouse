#!/usr/bin/env bash
# Tags: long

# shellcheck disable=SC2015

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS mt";
$CLICKHOUSE_CLIENT --query "CREATE TABLE mt (n Int8, m Int8) ENGINE=MergeTree ORDER BY n PARTITION BY 0 < n SETTINGS old_parts_lifetime=0";

function thread_insert_commit()
{
    for i in {1..50}; do
        $CLICKHOUSE_CLIENT --multiquery --query "
        BEGIN TRANSACTION;
        INSERT INTO mt VALUES /* ($i, $1) */ ($i, $1);
        INSERT INTO mt VALUES /* (-$i, $1) */ (-$i, $1);
        COMMIT;";
    done
}

function thread_insert_rollback()
{
    for _ in {1..50}; do
        $CLICKHOUSE_CLIENT --multiquery --query "
        BEGIN TRANSACTION;
        INSERT INTO mt VALUES /* (42, $1) */ (42, $1);
        ROLLBACK;";
    done
}

function thread_select()
{
    while true; do
        # Result of `uniq | wc -l` must be 1 if the first and the last queries got the same result
        $CLICKHOUSE_CLIENT --multiquery --query "
        BEGIN TRANSACTION;
        SELECT arraySort(groupArray(n)), arraySort(groupArray(m)), arraySort(groupArray(_part)) FROM mt;
        SELECT throwIf((SELECT sum(n) FROM mt) != 0) FORMAT Null;
        SELECT throwIf((SELECT count() FROM mt) % 2 != 0) FORMAT Null;
        SELECT arraySort(groupArray(n)), arraySort(groupArray(m)), arraySort(groupArray(_part)) FROM mt;
        COMMIT;" | uniq | wc -l | grep -v "^1$" ||:
    done
}

thread_insert_commit 1 & PID_1=$!
thread_insert_commit 2 & PID_2=$!
thread_insert_rollback 3 & PID_3=$!
thread_select & PID_4=$!
wait $PID_1 && wait $PID_2 && wait $PID_3
kill -TERM $PID_4
wait
wait_for_queries_to_finish

$CLICKHOUSE_CLIENT --multiquery --query "
BEGIN TRANSACTION;
SELECT count(), sum(n), sum(m=1), sum(m=2), sum(m=3) FROM mt;";

$CLICKHOUSE_CLIENT --query "SELECT count(), sum(n), sum(m=1), sum(m=2), sum(m=3) FROM mt;"

$CLICKHOUSE_CLIENT --query "DROP TABLE mt";
