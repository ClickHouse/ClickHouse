#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

for i in {1..1000}; do echo "ddl $i"; echo "drop table if exists test.view" | $CLICKHOUSE_CLIENT; echo "create view test.view as select count(*),database,table from system.columns group by database,table" | $CLICKHOUSE_CLIENT; done &
for i in {1..1000}; do echo "select $i"; echo "select * from test.view order by table" | $CLICKHOUSE_CLIENT >/dev/null 2>&1 || true; done &

wait
