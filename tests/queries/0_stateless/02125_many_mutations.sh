#!/usr/bin/env bash
# Tags: long, no-tsan, no-debug, no-asan, no-msan, no-ubsan

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# "max_parts_to_merge_at_once = 1" prevents merges to start in background before our own OPTIMIZE FINAL

$CLICKHOUSE_CLIENT --multiquery -q "
drop table if exists many_mutations;
create table many_mutations (x UInt32, y UInt32) engine = MergeTree order by x settings number_of_mutations_to_delay = 0, number_of_mutations_to_throw = 0, max_parts_to_merge_at_once = 1;
insert into many_mutations values (0, 0), (1, 1);
system stop merges many_mutations;
select x, y from many_mutations order by x;
"

job()
{
   yes "alter table many_mutations update y = y + 1 where 1;" | head -n 1000 | $CLICKHOUSE_CLIENT --multiquery
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

$CLICKHOUSE_CLIENT --multiquery -q "
select count() from system.mutations where database = currentDatabase() and table = 'many_mutations' and not is_done;
system start merges many_mutations;
optimize table many_mutations final SETTINGS optimize_throw_if_noop = 1;
select count() from system.mutations where database = currentDatabase() and table = 'many_mutations' and not is_done;
select x, y from many_mutations order by x;
drop table many_mutations;
"
