#!/usr/bin/env bash
# Tags: no-fasttest
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS parallel_ddl"

function query()
{
    local it=0
    TIMELIMIT=30
    while [ $SECONDS -lt "$TIMELIMIT" ] && [ $it -lt 50 ];
    do
        it=$((it+1))
        ${CLICKHOUSE_CLIENT} --query "CREATE TABLE IF NOT EXISTS parallel_ddl(a Int) ENGINE = Memory"
        ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS parallel_ddl"
    done
}

for _ in {1..2}; do
    query &
done

wait

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS parallel_ddl"
