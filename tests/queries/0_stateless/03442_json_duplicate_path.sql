set type_json_allow_duplicated_key_with_literal_and_nested_object=0;

select '{"a" : 42, "a" : {"b" : 42}}'::JSON; -- {serverError INCORRECT_DATA}
select '{"a" : {"b" : 42}, "a" : 42}'::JSON; -- {serverError INCORRECT_DATA}
select '{"a" : 42, "a" : {"b" : 42}}'::JSON settings type_json_skip_duplicated_paths=1;
select '{"a" : {"b" : 42}, "a" : 42}'::JSON settings type_json_skip_duplicated_paths=1;

