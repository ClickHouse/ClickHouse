-- { echoOn }
select jsonMerge(null);
select jsonMerge('[1]'); -- { serverError ILLEGAL_JSON_OBJECT_FORMAT }
select jsonMerge('{"a":1}');
select jsonMerge('{"a":1}','{"name": "joey"}','{"name": "tom"}','{"name": "zoey"}');

select jsonMerge('{"a": "1","b": 2,"c": [true,{"qrdzkzjvnos": true,"yxqhipj": false,"oesax": "33o8_6AyUy"}]}', '{"c": "1"}');

select jsonMerge('{"a": "1","b": 2,"c": [true,"qrdzkzjvnos": true,"yxqhipj": false,"oesax": "33o8_6AyUy"}]}', '{"c": "1"}'); -- { serverError ILLEGAL_JSON_OBJECT_FORMAT }
