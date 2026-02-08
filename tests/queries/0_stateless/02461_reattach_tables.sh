#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

MY_CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=trace/g')

function check_if_detached_impl()
{
    query="$1"
    table="$2"
    ${MY_CLICKHOUSE_CLIENT} \
        --reattach_tables_before_query_execution=1  \
        --query "$query" 2>&1 \
        | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.$table"
}

function check_if_detached()
{
    check_if_detached_impl "$1" "$2"
    if [ $? -eq 0 ]; then
        echo "OK"
    else
        echo "FAIL"
    fi
}

function check_if_not_detached()
{
    check_if_detached_impl "$1" "$2"
    if [ $? -ne 0 ]; then
        echo "OK"
    else
        echo "FAIL"
    fi
}

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_2"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_1 (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_2 (a UInt64) ENGINE = MergeTree ORDER BY a"

check_if_detached "INSERT INTO t_reattach_1 VALUES (1)" "t_reattach_1"

check_if_detached "SELECT * FROM t_reattach_1" "t_reattach_1"
check_if_detached "SELECT * FROM t_reattach_1 JOIN t_reattach_2 USING a" "t_reattach_1"
check_if_detached "SELECT * FROM t_reattach_1 JOIN t_reattach_2 USING a" "t_reattach_2"

check_if_detached "INSERT INTO t_reattach_2 SELECT * FROM t_reattach_1" "t_reattach_1"
check_if_detached "INSERT INTO t_reattach_2 SELECT * FROM t_reattach_1" "t_reattach_2"

check_if_detached "EXISTS TABLE t_reattach_1" "t_reattach_1"
check_if_detached "SHOW CREATE TABLE t_reattach_1" "t_reattach_1"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_2"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_1 (a UInt64) ENGINE = Memory"

check_if_not_detached "INSERT INTO t_reattach_1 VALUES (55)" "t_reattach_1"
check_if_not_detached "SELECT * FROM t_reattach_1" "t_reattach_1"

${CLICKHOUSE_CLIENT} -q "SELECT * FROM t_reattach_1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_1"

${CLICKHOUSE_CLIENT} --reattach_tables_before_query_execution=1 -q "SELECT number FROM system.numbers LIMIT 1"
${CLICKHOUSE_CLIENT} --reattach_tables_before_query_execution=1 -q "SELECT number FROM system.numbers LIMIT 1"
