#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE_1}"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${CLICKHOUSE_DATABASE_1}"

$CLICKHOUSE_CLIENT --query "CREATE TABLE ${CLICKHOUSE_DATABASE_1}.t1 (x UInt64, s Array(Nullable(String))) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "CREATE TABLE ${CLICKHOUSE_DATABASE_1}.t2 (x UInt64, s Array(Nullable(String))) ENGINE = Memory"

function thread_detach_attach {
    local TIMELIMIT=$((SECONDS+20))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "DETACH DATABASE ${CLICKHOUSE_DATABASE_1}" 2>&1 | grep -v -F -e 'Received exception from server' -e 'Code: 219' -e 'Code: 741' -e '(query: '
        sleep 0.0$RANDOM
        $CLICKHOUSE_CLIENT --query "ATTACH DATABASE ${CLICKHOUSE_DATABASE_1}" 2>&1 | grep -v -F -e 'Received exception from server' -e 'Code: 82' -e 'Code: 741' -e '(query: '
        sleep 0.0$RANDOM
    done
}

function thread_rename {
    local TIMELIMIT=$((SECONDS+20))
    while [ $SECONDS -lt "$TIMELIMIT" ]
    do
        $CLICKHOUSE_CLIENT --query "RENAME TABLE ${CLICKHOUSE_DATABASE_1}.t1 TO ${CLICKHOUSE_DATABASE_1}.t2_tmp, ${CLICKHOUSE_DATABASE_1}.t2 TO ${CLICKHOUSE_DATABASE_1}.t1, ${CLICKHOUSE_DATABASE_1}.t2_tmp TO ${CLICKHOUSE_DATABASE_1}.t2" 2>&1 | grep -v -F -e 'Received exception from server' -e '(query: ' | grep -v -P 'Code: (81|60|57|521|741|159)'
        sleep 0.0$RANDOM
        $CLICKHOUSE_CLIENT --query "RENAME TABLE ${CLICKHOUSE_DATABASE_1}.t2 TO ${CLICKHOUSE_DATABASE_1}.t1, ${CLICKHOUSE_DATABASE_1}.t2_tmp TO ${CLICKHOUSE_DATABASE_1}.t2" 2>&1 | grep -v -F -e 'Received exception from server' -e '(query: ' | grep -v -P 'Code: (81|60|57|521|741|159)'
        sleep 0.0$RANDOM
        $CLICKHOUSE_CLIENT --query "RENAME TABLE ${CLICKHOUSE_DATABASE_1}.t2_tmp TO ${CLICKHOUSE_DATABASE_1}.t2" 2>&1 | grep -v -F -e 'Received exception from server' -e '(query: ' | grep -v -P 'Code: (81|60|57|521|741|159)'
        sleep 0.0$RANDOM
    done
}

thread_detach_attach &
thread_rename &
wait
sleep 1

$CLICKHOUSE_CLIENT --query "DETACH DATABASE IF EXISTS ${CLICKHOUSE_DATABASE_1}"
$CLICKHOUSE_CLIENT --query "ATTACH DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE_1}"
$CLICKHOUSE_CLIENT --query "DROP DATABASE ${CLICKHOUSE_DATABASE_1}"
