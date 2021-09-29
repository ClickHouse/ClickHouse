#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

declare -a engines=("MergeTree" "ReplicatedMergeTree('/test/01162/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX', '1')")

for engine in "${engines[@]}"
do
    $CLICKHOUSE_CLIENT -q "drop table if exists t"
    $CLICKHOUSE_CLIENT -q "create table t (n int) engine=$engine order by n"
    $CLICKHOUSE_CLIENT -q "insert into t values (1)"
    $CLICKHOUSE_CLIENT -q "insert into t values (2)"
    $CLICKHOUSE_CLIENT -q "select * from t order by n"
    $CLICKHOUSE_CLIENT --mutations_sync=1 -q "alter table t delete where n global in (select * from (select * from t where n global in (1::Int32)))"
    $CLICKHOUSE_CLIENT -q "select * from t order by n"
    $CLICKHOUSE_CLIENT --mutations_sync=1 -q "alter table t delete where n global in (select t1.n from t as t1 join t as t2 on t1.n=t2.n where t1.n global in (select 2::Int32))"
    $CLICKHOUSE_CLIENT -q "select count() from t"
    $CLICKHOUSE_CLIENT -q "drop table t"
done
