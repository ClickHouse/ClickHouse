#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

declare INSERT_QUERY_ID
declare IN_LIST_SUBQUERY_ID
declare WITH_SUBQUERY_SELECT_X_ID
declare WITH_SUBQUERY_SELECT_X_X_ID
declare WITH_SUBQUERY_SELECT_STAR_ID
declare WITH_SUBQUERY_SELECT_STAR_UNION_ID

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS mv;
    DROP TABLE IF EXISTS output;
    DROP TABLE IF EXISTS input;

    CREATE TABLE input (key Int) Engine=Null;
    CREATE TABLE output AS input Engine=Null;
    CREATE MATERIALIZED VIEW mv TO output SQL SECURITY NONE AS SELECT * FROM input;
"

for enable_analyzer in 0 1; do
    query_id="$(random_str 10)"
    INSERT_QUERY_ID=$query_id
    query="INSERT INTO input SELECT * FROM numbers(1)"
    echo "$query"
    $CLICKHOUSE_CLIENT --parallel_distributed_insert_select=0 --enable_analyzer "$enable_analyzer" --query_id "$query_id" -q "$query"

    query_id="$(random_str 10)"
    IN_LIST_SUBQUERY_ID=$query_id
    query="SELECT * FROM system.one WHERE dummy IN (SELECT * FROM system.one) FORMAT Null"
    echo "$query"
    $CLICKHOUSE_CLIENT --enable_analyzer "$enable_analyzer" --query_id "$query_id" -q "$query"

    query_id="$(random_str 10)"
    WITH_SUBQUERY_SELECT_X_ID=$query_id
    query="WITH (SELECT * FROM system.one) AS x SELECT x FORMAT Null"
    echo "$query"
    $CLICKHOUSE_CLIENT --enable_analyzer "$enable_analyzer" --query_id "$query_id" -q "$query"

    query_id="$(random_str 10)"
    WITH_SUBQUERY_SELECT_X_X_ID=$query_id
    query="WITH (SELECT * FROM system.one) AS x SELECT x, x FORMAT Null"
    echo "$query"
    $CLICKHOUSE_CLIENT --enable_analyzer "$enable_analyzer" --query_id "$query_id" -q "$query"

    query_id="$(random_str 10)"
    WITH_SUBQUERY_SELECT_STAR_ID=$query_id
    query="WITH x AS (SELECT * FROM system.one) SELECT * FROM x FORMAT Null"
    echo "$query"
    $CLICKHOUSE_CLIENT --enable_analyzer "$enable_analyzer" --query_id "$query_id" -q "$query"

    query_id="$(random_str 10)"
    WITH_SUBQUERY_SELECT_STAR_UNION_ID=$query_id
    query="WITH x AS (SELECT * FROM system.one) SELECT * FROM x UNION ALL SELECT * FROM x FORMAT Null"
    echo "$query"
    $CLICKHOUSE_CLIENT --enable_analyzer "$enable_analyzer" --query_id "$query_id" -q "$query"
    
    $CLICKHOUSE_CLIENT -m -q "SYSTEM FLUSH LOGS query_log"

    QUERY_IDS=(
        $INSERT_QUERY_ID $IN_LIST_SUBQUERY_ID $WITH_SUBQUERY_SELECT_X_ID
        $WITH_SUBQUERY_SELECT_X_X_ID $WITH_SUBQUERY_SELECT_STAR_ID $WITH_SUBQUERY_SELECT_STAR_UNION_ID
    )

    for qid in "${QUERY_IDS[@]}"; do
        $CLICKHOUSE_CLIENT -m -q "
            SELECT
                1 subquery,
                $enable_analyzer enable_analyzer,
                ProfileEvents['InsertQuery'] InsertQuery,
                ProfileEvents['SelectQuery'] SelectQuery,
                ProfileEvents['InsertQueriesWithSubqueries'] InsertQueriesWithSubqueries,
                ProfileEvents['SelectQueriesWithSubqueries'] SelectQueriesWithSubqueries,
                ProfileEvents['QueriesWithSubqueries'] QueriesWithSubqueries
            FROM system.query_log
            WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id = '$qid'
            FORMAT TSVWithNames;
        "
    done
done
