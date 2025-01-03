set enable_json_type=1;
set enable_analyzer=1;
select ['{"a":1}','{"a":1,"c":1}']::Array(JSON(max_dynamic_paths=2)) settings enable_json_type=1;

