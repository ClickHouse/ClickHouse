set allow_experimental_json_type = 1;
drop table if exists test;
create table test (json JSON(a UInt32, max_dynamic_paths=2)) engine=Memory;
insert into test values ('{"a" : 42, "b" : "Hello", "c" : [1, 2, 3], "d" : "2020-01-01", "e" : [{"f" : 42}]}');
insert into test values ('{"b" : "Hello", "c" : [1, 2, 3], "d" : "2020-01-01", "e" : [{"f" : 42}], "a" : 42}');
insert into test values ('{"c" : [1, 2, 3], "d" : "2020-01-01", "e" : [{"f" : 42}], "a" : 42, "b" : "Hello"}');
insert into test values ('{"d" : "2020-01-01", "e" : [{"f" : 42}], "a" : 42, "b" : "Hello", "c" : [1, 2, 3]}');
insert into test values ('{"e" : [{"f" : 42}], "a" : 42, "b" : "Hello", "c" : [1, 2, 3], "d" : "2020-01-01"}');
insert into test values ('{"a" : 42}'), ('{"b" : "Hello"}'), ('{"c" : [1, 2, 3]}'), ('{"d" : "2020-01-01"}'), ('{"e" : [{"f" : 42}]}');
insert into test values ('{"b" : "Hello"}'), ('{"c" : [1, 2, 3]}'), ('{"d" : "2020-01-01"}'), ('{"e" : [{"f" : 42}]}'), ('{"a" : 42}');
insert into test values ('{"c" : [1, 2, 3]}'), ('{"d" : "2020-01-01"}'), ('{"e" : [{"f" : 42}]}'), ('{"a" : 42}'), ('{"b" : "Hello"}');
insert into test values ('{"d" : "2020-01-01"}'), ('{"e" : [{"f" : 42}]}'), ('{"a" : 42}'), ('{"b" : "Hello"}'), ('{"c" : [1, 2, 3]}');
insert into test values ('{"e" : [{"f" : 42}]}'), ('{"a" : 42}'), ('{"b" : "Hello"}'), ('{"c" : [1, 2, 3]}'), ('{"d" : "2020-01-01"}');
insert into test values ('{"a" : 42}');
insert into test values ('{"b" : "Hello"}');
insert into test values ('{"c" : [1, 2, 3]}');
insert into test values ('{"d" : "2020-01-01"}');
insert into test values ('{"e" : [{"f" : 42}]}');

insert into test values ('{"a" : 42, "c" : "Hello", "d" : [1, 2, 3], "e" : "2020-01-01", "b" : [{"f" : 42}]}');
insert into test values ('{"c" : "Hello", "d" : [1, 2, 3], "e" : "2020-01-01", "b" : [{"f" : 42}], "a" : 42}');
insert into test values ('{"d" : [1, 2, 3], "e" : "2020-01-01", "b" : [{"f" : 42}], "a" : 42, "c" : "Hello"}');
insert into test values ('{"e" : "2020-01-01", "b" : [{"f" : 42}], "a" : 42, "c" : "Hello", "d" : [1, 2, 3]}');
insert into test values ('{"b" : [{"f" : 42}], "a" : 42, "c" : "Hello", "d" : [1, 2, 3], "e" : "2020-01-01"}');
insert into test values ('{"a" : 42}'), ('{"c" : "Hello"}'), ('{"d" : [1, 2, 3]}'), ('{"e" : "2020-01-01"}'), ('{"b" : [{"f" : 42}]}');
insert into test values ('{"c" : "Hello"}'), ('{"d" : [1, 2, 3]}'), ('{"e" : "2020-01-01"}'), ('{"b" : [{"f" : 42}]}'), ('{"a" : 42}');
insert into test values ('{"d" : [1, 2, 3]}'), ('{"e" : "2020-01-01"}'), ('{"b" : [{"f" : 42}]}'), ('{"a" : 42}'), ('{"c" : "Hello"}');
insert into test values ('{"e" : "2020-01-01"}'), ('{"b" : [{"f" : 42}]}'), ('{"a" : 42}'), ('{"c" : "Hello"}'), ('{"d" : [1, 2, 3]}');
insert into test values ('{"b" : [{"f" : 42}]}'), ('{"a" : 42}'), ('{"c" : "Hello"}'), ('{"d" : [1, 2, 3]}'), ('{"e" : "2020-01-01"}');
insert into test values ('{"a" : 42}');
insert into test values ('{"c" : "Hello"}');
insert into test values ('{"d" : [1, 2, 3]}');
insert into test values ('{"e" : "2020-01-01"}');
insert into test values ('{"b" : [{"f" : 42}]}');

select uniqExact(json) from test;
select count(), json from test group by json order by toString(json);

drop table test;
