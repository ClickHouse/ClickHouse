#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS d_create_race"

function query()
{
    for i in {1..100}; do
        ${CLICKHOUSE_CLIENT} --query "CREATE DATABASE IF NOT EXISTS d_create_race"
        ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS d_create_race"  
    done
}

for i in {1..2}; do
    query &
done

wait

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS d_create_race"
