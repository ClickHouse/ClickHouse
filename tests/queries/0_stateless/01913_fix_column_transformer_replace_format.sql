drop view if exists my_view;
drop table if exists my_table;

create table my_table(Id UInt32, Object Nested(Key UInt8, Value String)) engine MergeTree order by Id;
create view my_view as select * replace arrayMap(x -> x + 1,`Object.Key`) as `Object.Key` from my_table;

show create my_view;

drop view my_view;
drop table my_table;
