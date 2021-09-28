#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

declare -a engines=("MergeTree" "ReplicatedMergeTree('/test/01162/$CLICKHOUSE_DATABASE', '1')")

for engine in "${engines[@]}"
do
    $CLICKHOUSE_CLIENT -q "drop table if exists t"
    $CLICKHOUSE_CLIENT -q "create table t (n int) engine=$engine order by n"
    $CLICKHOUSE_CLIENT -q "insert into t values (1)"
    $CLICKHOUSE_CLIENT -q "insert into t values (2)"
    $CLICKHOUSE_CLIENT -q "select * from t order by n"
    $CLICKHOUSE_CLIENT -q "alter table t delete where n global in (select * from (select * from t))"
    $CLICKHOUSE_CLIENT -q "select count() from t"
    $CLICKHOUSE_CLIENT -q "drop table t"
done
