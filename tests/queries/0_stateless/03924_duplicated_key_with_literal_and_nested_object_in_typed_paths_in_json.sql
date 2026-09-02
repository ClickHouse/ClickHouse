set type_json_allow_duplicated_key_with_literal_and_nested_object=1;
select '{"a" : 42, "a" : {"b" : "42"}}'::JSON(a UInt32, a.b UInt32);
select '{"a" : 42, "a" : {"b" : "42"}}'::JSON::JSON(a UInt32, a.b UInt32);
select arrayJoin(['{"a" : 42}', '{"a" : {"b" : 42}}'])::JSON(a UInt32, a.b UInt32);

