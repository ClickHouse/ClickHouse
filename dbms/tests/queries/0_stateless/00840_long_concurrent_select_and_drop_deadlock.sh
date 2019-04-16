#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

for i in {1..200}; do echo "drop table if exists view" | $CLICKHOUSE_CLIENT; echo "create view view as select count(*),database,table from system.columns group by database,table" | $CLICKHOUSE_CLIENT; done &
for i in {1..500}; do echo "select * from view order by table" | $CLICKHOUSE_CLIENT >/dev/null 2>&1 || true; done &

wait

echo "drop table view" | $CLICKHOUSE_CLIENT

echo 'did not deadlock'
