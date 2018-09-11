#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS db_create_race"

for i in {1..10}; do
    ${CLICKHOUSE_CLIENT} --query "CREATE DATABASE IF NOT EXISTS db_create_race" &    
done

wait

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS db_create_race"
