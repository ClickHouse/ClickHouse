#!/usr/bin/env bash
# Tags: long, no-tsan, no-debug, no-asan, no-msan, no-ubsan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# "max_parts_to_merge_at_once = 1" prevents merges to start in background before our own OPTIMIZE FINAL

$CLICKHOUSE_CLIENT -q "create table many_mutations (x UInt32, y UInt32) engine = MergeTree order by x settings number_of_mutations_to_delay = 0, number_of_mutations_to_throw = 0, max_parts_to_merge_at_once = 1"
$CLICKHOUSE_CLIENT -q "insert into many_mutations select number, number + 1 from numbers(2000)"
$CLICKHOUSE_CLIENT -q "system stop merges many_mutations"

$CLICKHOUSE_CLIENT -q "select count() from many_mutations"

job()
{
   for i in {1..1000}
   do
      $CLICKHOUSE_CLIENT -q "alter table many_mutations delete where y = ${i} * 2 settings mutations_sync=0"
   done
}

job &
job &
job &
job &
job &
job &
job &
job &
job &
job &
job &
job &
job &
job &
job &
job &
job &
job &
job &
job &

wait

$CLICKHOUSE_CLIENT -q "select count() from system.mutations where database = currentDatabase() and table = 'many_mutations' and not is_done"
$CLICKHOUSE_CLIENT -q "system start merges many_mutations"
$CLICKHOUSE_CLIENT -q "optimize table many_mutations final" --optimize_throw_if_noop 1
$CLICKHOUSE_CLIENT -q "system flush logs"
$CLICKHOUSE_CLIENT -q "select count() from system.mutations where database = currentDatabase() and table = 'many_mutations' and not is_done"
$CLICKHOUSE_CLIENT -q "select count() from many_mutations"
$CLICKHOUSE_CLIENT -q "select * from system.part_log where database = currentDatabase() and table == 'many_mutations' and peak_memory_usage > 1e9"
