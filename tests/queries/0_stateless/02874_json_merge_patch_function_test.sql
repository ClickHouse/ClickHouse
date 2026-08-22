-- Tags: no-fasttest
select jsonMergePatch(null);
select jsonMergePatch('{"a":1}');
select jsonMergePatch('{"a":1}', '{"b":1}');
select jsonMergePatch('{"a":1}', '{"b":1}', '{"c":[1,2]}');
select jsonMergePatch('{"a":1}', '{"b":1}', '{"c":[{"d":1},2]}');
select jsonMergePatch('{"a":1}','{"name": "joey"}','{"name": "tom"}','{"name": "zoey"}');
select jsonMergePatch('{"a": "1","b": 2,"c": [true,{"qrdzkzjvnos": true,"yxqhipj": false,"oesax": "33o8_6AyUy"}]}', '{"c": "1"}');
select jsonMergePatch('{"a": {"b": 1, "c": 2}}', '{"a": {"b": [3, 4]}}');
select jsonMergePatch('{ "a": 1, "b":2 }','{ "a": 3, "c":4 }','{ "a": 5, "d":6 }');
select jsonMergePatch('{"a":1, "b":2}', '{"b":null}');

select jsonMergePatch('[1]'); -- { serverError BAD_ARGUMENTS }
select jsonMergePatch('{"a": "1","b": 2,"c": [true,"qrdzkzjvnos": true,"yxqhipj": false,"oesax": "33o8_6AyUy"}]}', '{"c": "1"}'); -- { serverError BAD_ARGUMENTS }

drop table if exists t_json_merge;
create table t_json_merge (id UInt64, s1 String, s2 String) engine = Memory;

insert into t_json_merge select number, format('{{ "k{0}": {0} }}', toString(number * 2)), format('{{ "k{0}": {0} }}', toString(number * 2 + 1)) from numbers(5);
insert into t_json_merge select number, format('{{ "k{0}": {0} }}', toString(number * 2)), format('{{ "k{0}": {0}, "k{1}": 222 }}', toString(number * 2 + 1), toString(number * 2)) from numbers(5, 5);

select jsonMergePatch(s1, s2) from t_json_merge ORDER BY id;

drop table t_json_merge;
