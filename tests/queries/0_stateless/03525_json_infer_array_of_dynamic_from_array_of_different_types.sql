set enable_analyzer=1;

desc format(JSONEachRow, '{"a" : [42, "hello", [1, 2, 3]]}');
select a, toTypeName(a), arrayMap(x -> dynamicType(x), a) from format(JSONEachRow, '{"a" : [42, "hello", [1, 2, 3]]}');
desc format(JSONEachRow, '{"a" : [42, "hello"]}');
select a, toTypeName(a), arrayMap(x -> dynamicType(x), a) from format(JSONEachRow, '{"a" : [42, "hello"]}');
desc format(JSONEachRow, '{"a" : [42, "hello", {"b" : 42}]}');
select a, toTypeName(a), arrayMap(x -> dynamicType(x), a) from format(JSONEachRow, '{"a" : [42, "hello", {"b" : 42}]}');

select '{"a" : [42, "hello", [1, 2, 3]]}'::JSON as json, JSONAllPathsWithTypes(json), dynamicType(json.a), json.a.:`Array(Dynamic)`.Int64, json.a.:`Array(Dynamic)`.String, json.a.:`Array(Dynamic)`.`Array(Nullable(Int64))`;
select '{"a" : [42, "hello", [1, 2, 3], {"b" : 42}]}'::JSON as json, JSONAllPathsWithTypes(json), dynamicType(json.a), json.a.:`Array(Dynamic)`.Int64, json.a.:`Array(Dynamic)`.String, json.a.:`Array(Dynamic)`.`Array(Nullable(Int64))`,  json.a.:`Array(Dynamic)`.JSON.b.:Int64;
select '{"a" : [42, "hello", [1, 2, 3]]}'::JSON::JSON(a Array(String)) as json, JSONAllPathsWithTypes(json);
