drop table if exists test;
create table test (json JSON) engine=Memory;
insert into test values ('{"a" : 1}'), ('{"a" : 2}'), ('{"a" : 2}'), ('{"a" : 4}');
select json, materialize('') from test order by all asc;
drop table test;

