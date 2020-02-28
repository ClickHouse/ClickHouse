drop table if exists mt_compact;

-- Checks that granularity correctly computed from small parts.

create table mt_compact(a Int, s String) engine = MergeTree order by a
settings min_rows_for_wide_part = 1000,
index_granularity = 14;

system stop merges mt_compact;
set max_block_size = 1;
set min_insert_block_size_rows=1;
insert into mt_compact select number, 'aaa' from numbers(100);

select count() from system.parts where table = 'mt_compact' and database = currentDatabase() and active;

system start merges mt_compact;
optimize table mt_compact final;

select count(), sum(marks) from system.parts where table = 'mt_compact' and database = currentDatabase() and active;

drop table mt_compact;
