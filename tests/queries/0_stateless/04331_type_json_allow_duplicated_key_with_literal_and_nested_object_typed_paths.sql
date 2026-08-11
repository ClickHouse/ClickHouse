set enable_analyzer=1;
set type_json_allow_duplicated_key_with_literal_and_nested_object=1;

-- Basic: typed path with literal first, object second
select '{"a" : 42, "a" : {"b" : 42}}'::JSON(a Int32) as json, json.a, json.a.b, json.^a, json.@a;

-- Basic: typed path with object first, literal second
select '{"a" : {"b" : 42}, "a" : 42}'::JSON(a Int32) as json, json.a, json.a.b, json.^a, json.@a;

-- Typed path for nested key
select '{"a" : {"b" : 42, "b" : {"c" : 42}}}'::JSON(a.b Int32) as json, json.a.b, json.a.b.c, json.@a.b;

-- Multiple typed paths
select '{"a" : 42, "a" : {"b" : 42}, "c" : "hello", "c" : {"d" : 1}}'::JSON(a Int32, c String) as json, json.a, json.c, json.a.b, json.c.d, json.@a, json.@c;

-- Typed path with Nullable type
select '{"a" : 42, "a" : {"b" : 42}}'::JSON(a Nullable(Int32)) as json, json.a, json.a.b, json.@a;

-- Same-type duplicates should still fail
select '{"a" : 42, "a" : 43}'::JSON(a Int32); -- {serverError INCORRECT_DATA}
select '{"a" : {"b" : 1}, "a" : {"c" : 2}}'::JSON(a Int32); -- {serverError INCORRECT_DATA}

-- Three duplicates (literal + object + literal) should still fail
select '{"a" : 42, "a" : {"b" : 42}, "a" : 43}'::JSON(a Int32); -- {serverError INCORRECT_DATA}

-- Without the setting, typed paths should still fail on duplicate keys
set type_json_allow_duplicated_key_with_literal_and_nested_object=0;
select '{"a" : 42, "a" : {"b" : 42}}'::JSON(a Int32); -- {serverError INCORRECT_DATA}
