#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS db_create_race"

function query()
{
    for i in {1..100}; do
        ${CLICKHOUSE_CLIENT} --query "CREATE TABLE IF NOT EXISTS db_create_race(a Int) ENGINE = Memory"
        ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS db_create_race"  
        echo i > 2
    done
}

for i in {1..2}; do
    query &
done

wait

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS db_create_race"
