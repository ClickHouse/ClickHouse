create database if not exists test;
drop table if exists test.tab;
create table test.tab (key UInt64, arr Array(UInt64)) Engine = MergeTree order by key;
insert into test.tab values (1, [1]);
insert into test.tab values (2, [2]);
select 'all';
select * from test.tab order by key;
select 'key, arrayJoin(arr) in (1, 1)';
select key, arrayJoin(arr) as val from test.tab where (key, val) in (1, 1);
select 'key, arrayJoin(arr) in ((1, 1), (2, 2))';
select key, arrayJoin(arr) as val from test.tab where (key, val) in ((1, 1), (2, 2)) order by key;
select '(key, left array join arr) in (1, 1)';
select key from test.tab left array join arr as val where (key, val) in (1, 1);
select '(key, left array join arr) in ((1, 1), (2, 2))';
select key from test.tab left array join arr as val where (key, val) in ((1, 1), (2, 2)) order by key;

