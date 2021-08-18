drop table if exists test;
create table test (name String, uuid UUID) engine=Memory();
insert into test select '1', '00000000-0000-0000-0000-000000000000';
insert into test select '2', '00000000-0000-0000-0000-000000000000';
insert into test select '3', '00000000-0000-0000-0000-000000000000';
insert into test select '4', '00000000-0000-0000-0000-000000000000';
insert into test select '5', '00000000-0000-0000-0000-000000000000';

-- { echo }
select count() from (select * from test union distinct select * from test);
select count() from (select * from test union distinct select * from test union all select * from test);

select uuid from test union distinct select uuid from test;
select uuid from test union distinct select uuid from test union all select uuid from test where name = '1';
select uuid from (select * from test union distinct select * from test);

