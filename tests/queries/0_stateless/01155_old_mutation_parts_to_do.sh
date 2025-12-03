#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree
# no-fasttest: Long sleep
# no-shared-merge-tree: doesn't rely on max_replicated_mutations_in_queue

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib


${CLICKHOUSE_CLIENT} -q "drop table if exists rmt;"

${CLICKHOUSE_CLIENT} -q "create table rmt (n int, m int, s String) engine=ReplicatedMergeTree('/test/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rmt', '1')
order by n settings max_replicated_mutations_in_queue=0;"

${CLICKHOUSE_CLIENT} -q "insert into rmt values (1, 1, '2');"     # 0_0_0_0
${CLICKHOUSE_CLIENT} --mutations_sync=0 -q "alter table rmt update m = m*toInt8(s) where 1;"  # 0000000000
${CLICKHOUSE_CLIENT} -q "insert into rmt values (2, 3, '5');"     # 0_2_2_0
${CLICKHOUSE_CLIENT} --mutations_sync=0 -q "alter table rmt update m = m*toInt8(s) where 1;"  # 0000000001
${CLICKHOUSE_CLIENT} -q "insert into rmt values (3, 7, 'fail');"  # 0_4_4_0
${CLICKHOUSE_CLIENT} --mutations_sync=0 -q "alter table rmt update m = m*toInt8(s) where n=3;"  # 0000000002, will fail to mutate 0_4_4_0 to 0_4_4_0_5
${CLICKHOUSE_CLIENT} -q "insert into rmt values (4, 11, '13');"   # 0_6_6_0

${CLICKHOUSE_CLIENT} -q "alter table rmt modify setting max_replicated_mutations_in_queue=1;"
sleep 5 # test does not rely on this, but it may help to reproduce a bug

${CLICKHOUSE_CLIENT} -q "kill mutation where database=currentDatabase() and table='rmt' and mutation_id='0000000002'";
${CLICKHOUSE_CLIENT} -q "system sync replica rmt;"

# now check that mutations 0 and 1 are finished
wait_for_mutation "rmt" "0000000001"
${CLICKHOUSE_CLIENT} -q "select * from rmt order by n;"
${CLICKHOUSE_CLIENT} -q "select mutation_id, command, parts_to_do_names, parts_to_do, is_done from system.mutations where database=currentDatabase() and table='rmt';"

${CLICKHOUSE_CLIENT} -q "drop table rmt;"
