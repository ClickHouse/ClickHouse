set enable_json_type=1;
set input_format_json_infer_array_of_dynamic_from_array_of_different_types=0;

select materialize('{"a" : [[1, {}], null]}')::JSON as json, getSubcolumn(json, 'a'), dynamicType(getSubcolumn(json, 'a'));
