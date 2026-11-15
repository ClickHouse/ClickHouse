set enable_json_type=1;

create table test (json JSON) engine=Memory;
insert into test values ('{}'), ('{"a" : 42}'), ('{"b" : {"c" : 42}}');
select json, notEmpty(json) from test;
drop table test;

create table test (json JSON(a UInt32)) engine=Memory;
insert into test values ('{}'), ('{"a" : 42}'), ('{"b" : {"c" : 42}}');
select json, notEmpty(json) from test;
drop table test;

create table test (json JSON(max_dynamic_paths=1)) engine=Memory;
insert into test values ('{}'), ('{"a" : 42}'), ('{"b" : {"c" : 42}}');
select json, notEmpty(json) from test;
drop table test;

create table test (json JSON(max_dynamic_paths=0)) engine=Memory;
insert into test values ('{}'), ('{"a" : 42}'), ('{"b" : {"c" : 42}}');
select json, notEmpty(json) from test;
drop table test;


