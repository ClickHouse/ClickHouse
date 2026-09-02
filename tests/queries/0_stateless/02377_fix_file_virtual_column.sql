drop table if exists test_02377;
create table test_02377 (n UInt32, s String) engine=File(CSVWithNames);
insert into test_02377 values(1, 's') (2, 'x') (3, 'y');
select * from test_02377 order by n;
select *, _path, _file from test_02377 format Null;
select _path, _file from test_02377 format Null;
drop table test_02377;
