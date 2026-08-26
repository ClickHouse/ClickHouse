#!/usr/bin/env bash
# Tags: long, no-ordinary-database, no-encrypted-storage

# shellcheck disable=SC2015
# shellcheck disable=SC2119

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

TIMEOUT=30

LAST_ITERATION_FILE_1="${CLICKHOUSE_TMP}/last_iteration_1_$$"
LAST_ITERATION_FILE_2="${CLICKHOUSE_TMP}/last_iteration_2_$$"
touch $LAST_ITERATION_FILE_1
touch $LAST_ITERATION_FILE_2

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS mt"
$CLICKHOUSE_CLIENT --query "CREATE TABLE mt (n Int8, m Int8) ENGINE=MergeTree ORDER BY n PARTITION BY 0 < n SETTINGS old_parts_lifetime=0"

function thread_insert_commit()
{
    for i in {1..50}; do
        [[ $SECONDS -ge $TIMEOUT ]] && break
        $CLICKHOUSE_CLIENT --query "
        BEGIN TRANSACTION;
        INSERT INTO mt VALUES /* ($i, $1) */ ($i, $1);
        INSERT INTO mt VALUES /* (-$i, $1) */ (-$i, $1);
        COMMIT;"
        echo $i > "${CLICKHOUSE_TMP}/last_iteration_${1}_$$"
    done
}

function thread_insert_rollback()
{
    for _ in {1..50}; do
        [[ $SECONDS -ge $TIMEOUT ]] && break
        $CLICKHOUSE_CLIENT --query "
        BEGIN TRANSACTION;
        INSERT INTO mt VALUES /* (42, $1) */ (42, $1);
        ROLLBACK;"
    done
}

function thread_select()
{
    while true; do
        [[ $SECONDS -ge $TIMEOUT ]] && break
        # The first and the last queries must get the same result
        $CLICKHOUSE_CLIENT --query "
        BEGIN TRANSACTION;
        SET throw_on_unsupported_query_inside_transaction=0;
        CREATE TEMPORARY TABLE tmp AS SELECT arraySort(groupArray(n)), arraySort(groupArray(m)), arraySort(groupArray(_part)) FROM mt FORMAT Null;
        SELECT throwIf((SELECT sum(n) FROM mt) != 0) FORMAT Null;
        SELECT throwIf((SELECT count() FROM mt) % 2 != 0) FORMAT Null;
        select throwIf((SELECT * FROM tmp) != (SELECT arraySort(groupArray(n)), arraySort(groupArray(m)), arraySort(groupArray(_part)) FROM mt)) FORMAT Null;
        COMMIT;"

        [[ $(cat "$LAST_ITERATION_FILE_1") -eq 50 && $(cat "$LAST_ITERATION_FILE_2") -eq 50 ]] && break
    done
}

thread_insert_commit 1 &
thread_insert_commit 2 &
thread_insert_rollback 3 &
thread_select &
wait

LAST_ITERATION_1=$(cat "$LAST_ITERATION_FILE_1")
LAST_ITERATION_2=$(cat "$LAST_ITERATION_FILE_2")
rm -f "$LAST_ITERATION_FILE_1" "$LAST_ITERATION_FILE_2"

$CLICKHOUSE_CLIENT --query "
BEGIN TRANSACTION;
SELECT (${LAST_ITERATION_1} + ${LAST_ITERATION_2}) * 2 = count(), sum(n), ${LAST_ITERATION_1} * 2 = sum(m=1), ${LAST_ITERATION_2} * 2 = sum(m=2), sum(m=3) FROM mt;"

$CLICKHOUSE_CLIENT --query "
SELECT (${LAST_ITERATION_1} + ${LAST_ITERATION_2}) * 2 = count(), sum(n), ${LAST_ITERATION_1} * 2 = sum(m=1), ${LAST_ITERATION_2} * 2 = sum(m=2), sum(m=3) FROM mt;"

$CLICKHOUSE_CLIENT --query "DROP TABLE mt"
