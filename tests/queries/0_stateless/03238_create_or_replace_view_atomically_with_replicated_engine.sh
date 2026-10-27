#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# with Replicated engine
${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DATABASE}_db ENGINE=Replicated('/test/clickhouse/db/${CLICKHOUSE_DATABASE}_db', 's1', 'r1')"

TIMEOUT=60

function create_or_replace_view_thread
{
    for _ in {1..15}; do
        ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d "CREATE OR REPLACE VIEW ${CLICKHOUSE_DATABASE}_db.test_view AS SELECT 'abcdef'" > /dev/null
        [[ $SECONDS -ge "$TIMEOUT" ]] && break
    done
}

function select_view_thread
{
    for _ in {1..15}; do
        ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d "SELECT * FROM ${CLICKHOUSE_DATABASE}_db.test_view" > /dev/null
        [[ $SECONDS -ge "$TIMEOUT" ]] && break
    done
}

${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d "CREATE OR REPLACE VIEW ${CLICKHOUSE_DATABASE}_db.test_view AS SELECT 'abcdef'" > /dev/null

select_view_thread &
select_view_thread &
select_view_thread &
select_view_thread &
select_view_thread &
select_view_thread &

create_or_replace_view_thread &
create_or_replace_view_thread &
create_or_replace_view_thread &
create_or_replace_view_thread &
create_or_replace_view_thread &

wait

${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}" -d "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_db SYNC" > /dev/null
