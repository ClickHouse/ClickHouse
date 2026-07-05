set enable_analyzer=1;

select '{"a" : [42.42, false]}'::JSON as json, dynamicType(json.a) settings input_format_json_read_bools_as_numbers=1;
select '{"a" : [42.42, false]}'::JSON as json, dynamicType(json.a) settings input_format_json_read_bools_as_numbers=0;
