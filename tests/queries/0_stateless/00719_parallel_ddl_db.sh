#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB_SUFFIX=${RANDOM}${RANDOM}${RANDOM}${RANDOM}
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS parallel_ddl_${DB_SUFFIX}"

function query()
{
    local it=0
    TIMELIMIT=30
    while [ $SECONDS -lt "$TIMELIMIT" ] && [ $it -lt 50 ];
    do
        it=$((it+1))
        ${CLICKHOUSE_CLIENT} --query "CREATE DATABASE IF NOT EXISTS parallel_ddl_${DB_SUFFIX}"
        ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS parallel_ddl_${DB_SUFFIX}"
    done
}

for _ in {1..2}; do
    query &
done

wait

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS parallel_ddl_${DB_SUFFIX}"
