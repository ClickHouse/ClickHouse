-- { echo }
--- ensure that input_format_null_as_default allow writes to Nullable columns too
select * from format(JSONEachRow, 'payload Tuple(pull_request Tuple(merged_by Tuple(login Nullable(String))))', '{"payload" : {"pull_request": {"merged_by": {"login": "root"}}}}') settings input_format_null_as_default=1;
select * from format(JSONEachRow, 'payload Tuple(pull_request Tuple(merged_by Tuple(login Nullable(String))))', '{"payload" : {"pull_request": {"merged_by": null}}}') settings input_format_null_as_default=1;
--- tuple
select * from format(JSONEachRow, 'payload Tuple(pull_request Tuple(merged_by Tuple(login String)))', '{"payload" : {"pull_request": {"merged_by": {"login": "root"}}}}') settings input_format_null_as_default=0;
select * from format(JSONEachRow, 'payload Tuple(pull_request Tuple(merged_by Tuple(login String)))', '{"payload" : {"pull_request": {"merged_by": {"login": "root"}}}}') settings input_format_null_as_default=1;
select * from format(JSONEachRow, 'payload Tuple(pull_request Tuple(merged_by Tuple(login String)))', '{"payload" : {}}') settings input_format_null_as_default=0;
select * from format(JSONEachRow, 'payload Tuple(pull_request Tuple(merged_by Tuple(login String)))', '{"payload" : {}}') settings input_format_null_as_default=1;
select * from format(JSONEachRow, 'payload Tuple(pull_request Tuple(merged_by Tuple(login String)))', '{"payload" : {"pull_request": {"merged_by": null}}}') settings input_format_null_as_default=0; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
select * from format(JSONEachRow, 'payload Tuple(pull_request Tuple(merged_by Tuple(login String)))', '{"payload" : {"pull_request": {"merged_by": null}}}') settings input_format_null_as_default=1;
--- map
set input_format_json_try_infer_named_tuples_from_objects=0;
set input_format_json_read_objects_as_strings=0;
select * from format(JSONEachRow, '{"payload" : {"pull_request": {"merged_by": {"login": "root"}}}}') settings input_format_null_as_default=0;
select * from format(JSONEachRow, '{"payload" : {"pull_request": {"merged_by": {"login": "root"}}}}') settings input_format_null_as_default=1;
select * from format(JSONEachRow, 'payload Map(String, String)', '{"payload" : {}}') settings input_format_null_as_default=0;
select * from format(JSONEachRow, 'payload Map(String, String)', '{"payload" : {}}') settings input_format_null_as_default=1;
select * from format(JSONEachRow, 'payload Map(String, Map(String, Map(String, String)))', '{"payload" : {"pull_request": {"merged_by": null}}}') settings input_format_null_as_default=0; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
select * from format(JSONEachRow, 'payload Map(String, Map(String, Map(String, String)))', '{"payload" : {"pull_request": {"merged_by": null}}}') settings input_format_null_as_default=1;
--- array
select * from format(JSONEachRow, 'payload Array(String)', '{"payload" : ["root"]}') settings input_format_null_as_default=0;
select * from format(JSONEachRow, 'payload Array(String)', '{"payload" : ["root"]}') settings input_format_null_as_default=1;
select * from format(JSONEachRow, 'payload Array(String)', '{"payload" : []}') settings input_format_null_as_default=0;
select * from format(JSONEachRow, 'payload Array(String)', '{"payload" : []}') settings input_format_null_as_default=1;
select * from format(JSONEachRow, 'payload Array(String)', '{"payload" : null}') settings input_format_null_as_default=0; -- { serverError CANNOT_READ_ARRAY_FROM_TEXT }
select * from format(JSONEachRow, 'payload Array(String)', '{"payload" : null}') settings input_format_null_as_default=1;
