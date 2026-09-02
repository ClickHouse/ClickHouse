set use_structure_from_insertion_table_in_table_functions = 1;

insert into table function file(concat(database(),'.data_02250.jsonl')) select (SELECT 1) settings engine_file_truncate_on_insert=1;

insert into table function file(concat(database(),'.data_02250.jsonl')) select NULL as x settings engine_file_truncate_on_insert=1;
drop table if exists test_02250;
create table test_02250 (x Nullable(UInt32)) engine=Memory();
insert into test_02250 select * from file(concat(database(),'.data_02250.jsonl'));
select * from test_02250;
drop table test_02250;
