insert into table function file('data_02250.jsonl') select NULL as x settings engine_file_truncate_on_insert=1;
drop table if exists test_02250;
create table test_02250 (x Nullable(UInt32)) engine=Memory();
insert into test_02250 select * from file('data_02250.jsonl');
select * from test_02250;
drop table test_02250;
