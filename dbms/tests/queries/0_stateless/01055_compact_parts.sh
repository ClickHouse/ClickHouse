#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

. $CURDIR/mergetree_mutations.lib

# Testing basic functionality with compact parts
${CLICKHOUSE_CLIENT} --query="drop table if exists mt_compact;"

${CLICKHOUSE_CLIENT} --query="create table mt_compact(a UInt64, b UInt64 DEFAULT a * a, s String, n Nested(x UInt32, y String), lc LowCardinality(String)) \
    engine = MergeTree \
    order by a partition by a % 10 \
    settings index_granularity = 8, \
    min_rows_for_wide_part = 10;"

${CLICKHOUSE_CLIENT} --query="insert into mt_compact (a, s, n.y, lc) select number, toString((number * 2132214234 + 5434543) % 2133443), ['a', 'b', 'c'], number % 2 ? 'bar' : 'baz' from numbers(90);"

${CLICKHOUSE_CLIENT} --query="select * from mt_compact order by a limit 10;"
${CLICKHOUSE_CLIENT} --query="select '=====================';"

${CLICKHOUSE_CLIENT} --query="select distinct part_type from system.parts where database = currentDatabase() and table = 'mt_compact' and active;"

${CLICKHOUSE_CLIENT} --query="insert into mt_compact (a, s, n.x, lc) select number % 3, toString((number * 2132214234 + 5434543) % 2133443), [1, 2], toString(number) from numbers(5);"

${CLICKHOUSE_CLIENT} --query="optimize table mt_compact final;"

${CLICKHOUSE_CLIENT} --query="select part_type, count() from system.parts where database = currentDatabase() and table = 'mt_compact' and active group by part_type; "
${CLICKHOUSE_CLIENT} --query="select * from mt_compact order by a, s limit 10;"
${CLICKHOUSE_CLIENT} --query="select '=====================';"

${CLICKHOUSE_CLIENT} --query="alter table mt_compact drop column n.y;"
${CLICKHOUSE_CLIENT} --query="alter table mt_compact add column n.y Array(String) DEFAULT ['qwqw'] after n.x;"
${CLICKHOUSE_CLIENT} --query="select * from mt_compact order by a, s limit 10;"
${CLICKHOUSE_CLIENT} --query="select '=====================';"

${CLICKHOUSE_CLIENT} --query="alter table mt_compact update b = 42 where 1;"

sleep 0.5
mutation_id=`${CLICKHOUSE_CLIENT} --query="SELECT max(mutation_id) FROM system.mutations WHERE table='mt_compact'"`
wait_for_mutation "mt_compact" "$mutation_id"

${CLICKHOUSE_CLIENT} --query="select * from mt_compact where a > 1 order by a, s limit 10;"
${CLICKHOUSE_CLIENT} --query="select '=====================';"

${CLICKHOUSE_CLIENT} --query="drop table if exists mt_compact;"
