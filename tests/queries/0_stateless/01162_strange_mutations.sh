#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

declare -a engines=("MergeTree order by n" "ReplicatedMergeTree('/test/01162/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX', '1') order by n" "Memory" "Join(ALL, FULL, n)")

$CLICKHOUSE_CLIENT -q "CREATE OR REPLACE VIEW t1 AS SELECT number * 10 AS id, number * 100 AS value FROM numbers(20)"

for engine in "${engines[@]}"
do
    $CLICKHOUSE_CLIENT -q "drop table if exists t"
    $CLICKHOUSE_CLIENT -q "create table t (n int) engine=$engine" 2>&1| grep -Ev "Removing leftovers from table|removed by another replica"
    $CLICKHOUSE_CLIENT -q "select engine from system.tables where database=currentDatabase() and name='t'"
    $CLICKHOUSE_CLIENT -q "insert into t values (1)"
    $CLICKHOUSE_CLIENT -q "insert into t values (2)"
    $CLICKHOUSE_CLIENT -q "select * from t order by n"
    $CLICKHOUSE_CLIENT --allow_nondeterministic_mutations=1 --mutations_sync=1 -q "alter table t
    delete where n global in (select * from (select * from t where n global in (1::Int32)))"
    $CLICKHOUSE_CLIENT -q "select * from t order by n"
    $CLICKHOUSE_CLIENT --allow_nondeterministic_mutations=1 --mutations_sync=1 -q "alter table t
    delete where n global in (select t1.n from t as t1 full join t as t2 on t1.n=t2.n where t1.n global in (select 2::Int32))"
    $CLICKHOUSE_CLIENT -q "select count() from t"
    $CLICKHOUSE_CLIENT -q "drop table t"

    $CLICKHOUSE_CLIENT -q "drop table if exists test"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE test ENGINE=$engine AS SELECT number + 100 AS n, 0 AS test FROM numbers(50)" 2>&1| grep -Ev "Removing leftovers from table|removed by another replica"
    $CLICKHOUSE_CLIENT -q "select count(), sum(n), sum(test) from test"
    if [[ $engine == *"ReplicatedMergeTree"* ]]; then
        $CLICKHOUSE_CLIENT -q "ALTER TABLE test
            UPDATE test = (SELECT groupArray(id) FROM t1 GROUP BY 'dummy')[n - 99] WHERE 1" 2>&1| grep -Fa "DB::Exception: " | grep -Fv "statement with subquery may be nondeterministic"
        $CLICKHOUSE_CLIENT --allow_nondeterministic_mutations=1 --mutations_sync=1 -q "ALTER TABLE test
                    UPDATE test = (SELECT groupArray(id) FROM t1)[n - 99] WHERE 1"
    elif [[ $engine == *"Join"* ]]; then
        $CLICKHOUSE_CLIENT -q "ALTER TABLE test
            UPDATE test = (SELECT groupArray(id) FROM t1)[n - 99] WHERE 1" 2>&1| grep -Fa "DB::Exception: " | grep -Fv "Table engine Join supports only DELETE mutations"
    else
        $CLICKHOUSE_CLIENT --mutations_sync=1 -q "ALTER TABLE test
            UPDATE test = (SELECT groupArray(id) FROM t1)[n - 99] WHERE 1"
    fi
    $CLICKHOUSE_CLIENT -q "select count(), sum(n), sum(test) from test"
    $CLICKHOUSE_CLIENT -q "drop table test"
done
