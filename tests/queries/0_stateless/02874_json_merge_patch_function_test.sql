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

select jsonMergePatch('[1]'); -- { serverError ILLEGAL_JSON_OBJECT_FORMAT }
select jsonMergePatch('{"a": "1","b": 2,"c": [true,"qrdzkzjvnos": true,"yxqhipj": false,"oesax": "33o8_6AyUy"}]}', '{"c": "1"}'); -- { serverError ILLEGAL_JSON_OBJECT_FORMAT }
