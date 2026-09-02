drop table if exists data_01279;

create table data_01279 (key String) Engine=TinyLog();
insert into data_01279 select reinterpretAsString(number) from numbers(100000);

set max_rows_to_group_by=10;
set group_by_overflow_mode='any';
set group_by_two_level_threshold=100;
select * from data_01279 group by key format Null;

drop table data_01279;
