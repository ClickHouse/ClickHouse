#!/usr/bin/env bash
# Tags: long, no-tsan, no-debug, no-asan, no-msan, no-ubsan, no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# "max_parts_to_merge_at_once = 1" prevents merges to start in background before our own OPTIMIZE FINAL

$CLICKHOUSE_CLIENT -q "
drop table if exists many_mutations;
create table many_mutations (x UInt32, y UInt32) engine = MergeTree order by x settings number_of_mutations_to_delay = 0, number_of_mutations_to_throw = 0, max_parts_to_merge_at_once = 1;
insert into many_mutations select number, number + 1 from numbers(2000);
system stop merges many_mutations;
"

$CLICKHOUSE_CLIENT -q "select count() from many_mutations"

job()
{
   for i in {1..1000}
   do
      echo "alter table many_mutations delete where y = ${i} * 2 settings mutations_sync = 0;"
   done | $CLICKHOUSE_CLIENT
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

# truncate before drop, avoid removing all the mutations (it's slow) in DatabaseCatalog's thread (may affect other tests)
$CLICKHOUSE_CLIENT -q "
select count() from system.mutations where database = currentDatabase() and table = 'many_mutations' and not is_done;
system start merges many_mutations;
optimize table many_mutations final SETTINGS optimize_throw_if_noop = 1;
alter table many_mutations update y = y + 1 where 1 settings mutations_sync=2;
system flush logs;
select count() from system.mutations where database = currentDatabase() and table = 'many_mutations' and not is_done;
select count() from many_mutations;
select * from system.part_log where database = currentDatabase() and table == 'many_mutations' and peak_memory_usage > 1e9;
truncate table many_mutations;
drop table many_mutations;
"
