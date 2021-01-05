drop table if exists data_01641;

create table data_01641 (key Int, value String) engine=MergeTree order by (key, repeat(value, 10)) settings old_parts_lifetime=0, min_bytes_for_wide_part=0;

-- peak memory usage is 170MiB
set max_memory_usage='200Mi';
system stop merges data_01641;
insert into data_01641 select number, toString(number) from numbers(toUInt64(120e6));

-- FIXME: this limit does not work
set max_memory_usage='10Mi';
system start merges data_01641;
optimize table data_01641 final;
