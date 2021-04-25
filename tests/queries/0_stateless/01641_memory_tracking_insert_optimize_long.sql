drop table if exists data_01641;

create table data_01641 (key Int, value String) engine=MergeTree order by (key, repeat(value, 10)) settings old_parts_lifetime=0, min_bytes_for_wide_part=0;

insert into data_01641 select number, toString(number) from numbers(120e6);

-- Definitely should fail and it proves that memory is tracked in OPTIMIZE query.
set max_memory_usage='10Mi';
optimize table data_01641 final; -- { serverError 241 }

drop table data_01641;
