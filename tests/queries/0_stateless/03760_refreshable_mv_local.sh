#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_03760"
${CLICKHOUSE_LOCAL} -q "
    create database $DB engine Atomic;
    create materialized view ${DB}.a refresh every 1 minute (t DateTime) engine Memory as select now() as t;
    select sum(ignore(*) + 1) from system.view_refreshes where database = '$DB';
    drop database $DB;
    select sum(ignore(*) + 1) from system.view_refreshes where database = '$DB';
"
