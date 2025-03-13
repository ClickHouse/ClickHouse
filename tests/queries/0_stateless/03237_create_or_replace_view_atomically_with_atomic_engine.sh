#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# with Atomic engine
$CLICKHOUSE_CLIENT --query "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE}_db ENGINE=Atomic"

function create_or_replace_view_thread
{
    for _ in {1..20}; do
        $CLICKHOUSE_CLIENT --query "CREATE OR REPLACE VIEW ${CLICKHOUSE_DATABASE}_db.test_view AS SELECT 'abcdef'" > /dev/null
    done
}
export -f create_or_replace_view_thread;

function select_view_thread
{
    for _ in {1..20}; do
        $CLICKHOUSE_CLIENT --query "SELECT * FROM ${CLICKHOUSE_DATABASE}_db.test_view" > /dev/null
    done
}
export -f select_view_thread;

$CLICKHOUSE_CLIENT --query "CREATE OR REPLACE VIEW ${CLICKHOUSE_DATABASE}_db.test_view AS SELECT 'abcdef'" > /dev/null

bash -c select_view_thread &
bash -c select_view_thread &
bash -c select_view_thread &
bash -c select_view_thread &
bash -c select_view_thread &
bash -c select_view_thread &
bash -c select_view_thread &
bash -c select_view_thread &
bash -c select_view_thread &
bash -c select_view_thread &
bash -c select_view_thread &
bash -c select_view_thread &

bash -c create_or_replace_view_thread &
bash -c create_or_replace_view_thread &
bash -c create_or_replace_view_thread &
bash -c create_or_replace_view_thread &
bash -c create_or_replace_view_thread &
bash -c create_or_replace_view_thread &
bash -c create_or_replace_view_thread &
bash -c create_or_replace_view_thread &
bash -c create_or_replace_view_thread &
bash -c create_or_replace_view_thread &

wait