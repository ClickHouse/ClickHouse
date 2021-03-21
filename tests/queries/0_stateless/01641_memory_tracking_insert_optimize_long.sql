drop table if exists data_01641;

create table data_01641 (key Int, value String) engine=MergeTree order by (key, repeat(value, 10)) settings old_parts_lifetime=0, min_bytes_for_wide_part=0;

-- peak memory usage is 170MiB
set max_memory_usage='200Mi';
system stop merges data_01641;
insert into data_01641 select number, toString(number) from numbers(120e6);

-- peak:
-- - is 21MiB if background merges already scheduled
-- - is ~60MiB otherwise
set max_memory_usage='80Mi';
system start merges data_01641;
optimize table data_01641 final;

-- definitely should fail
set max_memory_usage='1Mi';
optimize table data_01641 final; -- { serverError 241 }

drop table data_01641;
