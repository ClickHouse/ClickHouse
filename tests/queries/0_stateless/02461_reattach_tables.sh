#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test uses DETACH/ATTACH itself

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

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_cte"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_cte (a UInt64) ENGINE = MergeTree ORDER BY a"

# A real CTE (WITH name AS (subquery)) shadows a table with the same name, so the table is not used.
check_if_not_detached "WITH t_reattach_cte AS (SELECT 1) SELECT * FROM t_reattach_cte" "t_reattach_cte"

# A scalar WITH alias does NOT shadow a table name in FROM: `WITH (SELECT 1) AS t_reattach_cte SELECT * FROM
# t_reattach_cte` reads the real table, so it is detached.
check_if_detached "WITH (SELECT 1) AS t_reattach_cte SELECT * FROM t_reattach_cte" "t_reattach_cte"

# A CTE's own definition body may reference a real table with the same name (only the CTE currently being
# resolved is hidden), so the real table is read inside the body and detached.
check_if_detached "WITH t_reattach_cte AS (SELECT * FROM t_reattach_cte) SELECT * FROM t_reattach_cte" "t_reattach_cte"

# A CTE defined only in a nested subquery must NOT shadow the same name in an outer FROM clause.
check_if_detached "SELECT * FROM t_reattach_cte WHERE a IN (WITH t_reattach_cte AS (SELECT 1) SELECT * FROM t_reattach_cte)" "t_reattach_cte"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_cte"
