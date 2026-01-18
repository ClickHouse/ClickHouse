select '{"a" : 42, "a" : {"b" : 42}}'::JSON; -- {serverError INCORRECT_DATA}
select '{"a" : {"b" : 42}, "a" : 42}'::JSON; -- {serverError INCORRECT_DATA}
select '{"a" : 42, "a" : {"b" : 42}}'::JSON settings type_json_skip_duplicated_paths=1;
select '{"a" : {"b" : 42}, "a" : 42}'::JSON settings type_json_skip_duplicated_paths=1;

