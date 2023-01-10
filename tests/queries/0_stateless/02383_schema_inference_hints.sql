-- Tags: no-fasttest
desc format(JSONEachRow, '{"x" : 1, "y" : "String", "z" : "0.0.0.0" }') settings schema_inference_hints='x UInt8, z IPv4';
desc format(JSONEachRow, '{"x" : 1, "y" : "String"}\n{"z" : "0.0.0.0", "y" : "String2"}\n{"x" : 2}') settings schema_inference_hints='x UInt8, z IPv4';
desc format(JSONEachRow, '{"x" : null}') settings schema_inference_hints='x Nullable(UInt32)';
desc format(JSONEachRow, '{"x" : []}') settings schema_inference_hints='x Array(UInt32)';
desc format(JSONEachRow, '{"x" : {}}') settings schema_inference_hints='x Map(String, String)';

desc format(CSV, '1,"String","0.0.0.0"') settings schema_inference_hints='c1 UInt8, c3 IPv4';
desc format(CSV, '1,"String","0.0.0.0"') settings schema_inference_hints='x UInt8, z IPv4', column_names_for_schema_inference='x, y, z';
desc format(CSV, '\\N') settings schema_inference_hints='x Nullable(UInt32)', column_names_for_schema_inference='x';
