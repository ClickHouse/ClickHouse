set allow_experimental_object_type=1;
set allow_experimental_json_type=1;
set use_json_alias_for_old_object_type=0;
select materialize('{"a" : 42}')::JSON as json, toTypeName(json);
set use_json_alias_for_old_object_type=1;
select '{"a" : 42}'::JSON as json, toTypeName(json);
select '{"a" : 42}'::JSON(max_dynamic_paths=100) as json, toTypeName(json); -- {serverError BAD_ARGUMENTS}

set allow_experimental_object_type = 0;
select materialize('{"a" : 42}')::JSON as json, toTypeName(json);

