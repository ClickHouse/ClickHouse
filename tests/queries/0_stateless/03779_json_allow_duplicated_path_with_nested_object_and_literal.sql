set enable_analyzer=1;
set type_json_allow_duplicated_key_with_literal_and_nested_object=1;
select '{"a" : 42, "a" : {"b" : 42}}'::JSON as json, json.a, json.a.b, json.^a;
select '{"a" : {"b" : 42}, "a" : 42}'::JSON as json, json.a, json.a.b, json.^a;
select '{"a" : 42, "a" : {"b" : 42}, "a" : 42}'::JSON; -- {serverError INCORRECT_DATA}
select '{"a" : 42, "a" : {"b" : 42}, "a" : {"c" : 42}}'::JSON; -- {serverError INCORRECT_DATA}

