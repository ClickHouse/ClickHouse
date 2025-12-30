-- Tags: no-fasttest
set enable_json_type=1;

select materialize('{}')::JSON;
select materialize('{"a" : 42, "b" : "Hello"}')::JSON;
select materialize('{"a" : {"b" : {"c" : {"d" : 42}, "e" : 43}, "f" : 44}, "g" : 44}')::JSON;
select materialize('{"a" : {"b" : {"c" : {"d" : 42}, "e" : 43}, "f" : 44}, "g" : 44}')::JSON(a.b.c.d Bool);
select materialize('{"a" : {"b" : {"c" : {"d" : 42}, "e" : 43}, "f" : 44}, "g" : 44}')::JSON(SKIP a.b.c.d);
select materialize('{"a" : {"b" : {"c" : {"d" : 42}, "e" : 43}, "f" : 44}, "g" : 44}')::JSON(SKIP a.b.c);
select materialize('{"a" : {"b" : {"c" : {"d" : 42}, "e" : 43}, "f" : 44}, "g" : 44}')::JSON(SKIP a.b);
select materialize('{"a" : {"b" : {"c" : {"d" : 42}, "e" : 43}, "f" : 44}, "g" : 44}')::JSON(SKIP a);
select materialize('{"a" : {"b" : {"c" : {"d" : 42}, "e" : 43}, "f" : 44}, "g" : 44}')::JSON(SKIP REGEXP '.*a.*b');
select materialize('{"a" : {"b" : {"c" : {"d" : 42}, "e" : 43}, "f" : 44}, "g" : 44}')::JSON(SKIP REGEXP '.*a.*');
select materialize('{"a" : {"b" : {"c" : {"d" : 42}, "e" : 43}, "f" : 44}, "g" : 44}')::JSON(SKIP REGEXP '.*');

select materialize('{"a" : {"b" : {"c" : {"d" : 42}, "e" : 43}, "f" : 44}, "g" : 44}')::JSON as json, JSONAllPathsWithTypes(json), JSONDynamicPathsWithTypes(json), JSONSharedDataPathsWithTypes(json);
select materialize('{"a" : {"b" : {"c" : {"d" : 42}, "e" : 43}, "f" : 44}, "g" : 44}')::JSON(max_dynamic_paths = 2) as json, JSONAllPathsWithTypes(json), JSONDynamicPathsWithTypes(json), JSONSharedDataPathsWithTypes(json);
select materialize('{"a" : {"b" : {"c" : {"d" : 42}, "e" : 43}, "f" : 44}, "g" : 44}')::JSON(max_dynamic_paths = 1) as json, JSONAllPathsWithTypes(json), JSONDynamicPathsWithTypes(json), JSONSharedDataPathsWithTypes(json);
select materialize('{"a" : {"b" : {"c" : {"d" : 42}, "e" : 43}, "f" : 44}, "g" : 44}')::JSON(max_dynamic_paths = 0) as json, JSONAllPathsWithTypes(json), JSONDynamicPathsWithTypes(json), JSONSharedDataPathsWithTypes(json);
select materialize('{"a" : {"b" : {"c" : {"d" : 42}, "e" : 43}, "f" : 44}, "g" : 44}')::JSON(max_dynamic_paths = 2, max_dynamic_types=0) as json, JSONAllPathsWithTypes(json), JSONDynamicPathsWithTypes(json), JSONSharedDataPathsWithTypes(json);
select materialize('{"a" : {"b" : {"c" : {"d" : 42}, "e" : 43}, "f" : 44}, "g" : 44}')::JSON(max_dynamic_paths = 1, max_dynamic_types=0) as json, JSONAllPathsWithTypes(json), JSONDynamicPathsWithTypes(json), JSONSharedDataPathsWithTypes(json);
select materialize('{"a" : {"b" : {"c" : {"d" : 42}, "e" : 43}, "f" : 44}, "g" : 44}')::JSON(max_dynamic_paths = 0, max_dynamic_types=0) as json, JSONAllPathsWithTypes(json), JSONDynamicPathsWithTypes(json), JSONSharedDataPathsWithTypes(json);
