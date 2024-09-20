#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# with Atomic engine
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_db"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${CLICKHOUSE_DATABASE}_db ENGINE=Atomic"

function create_or_replace_view_thread
{
    for _ in {1..1000}; do
        $CLICKHOUSE_CLIENT --query "CREATE OR REPLACE VIEW ${CLICKHOUSE_DATABASE}_db.test_view AS SELECT 1"
    done
}

export -f create_or_replace_view_thread;

bash -c create_or_replace_view_thread 2> /dev/null &

$CLICKHOUSE_CLIENT --query "CREATE OR REPLACE VIEW ${CLICKHOUSE_DATABASE}_db.test_view AS SELECT 1"

for _ in {1..1000}; do
    $CLICKHOUSE_CLIENT --query "SELECT * FROM ${CLICKHOUSE_DATABASE}_db.test_view" > /dev/null
done

wait

# with Replicated engine
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_db"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE ${CLICKHOUSE_DATABASE}_db ENGINE=Replicated"

TIMEOUT=20
timeout $TIMEOUT bash -c create_or_replace_view_thread 2> /dev/null &

$CLICKHOUSE_CLIENT --query "CREATE OR REPLACE VIEW ${CLICKHOUSE_DATABASE}_db.test_view AS SELECT 1"

for _ in {1..1000}; do
    $CLICKHOUSE_CLIENT --query "SELECT * FROM ${CLICKHOUSE_DATABASE}_db.test_view" > /dev/null
done

wait