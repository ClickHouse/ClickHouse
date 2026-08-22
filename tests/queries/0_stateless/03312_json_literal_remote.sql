-- Tags: no-fasttest

set enable_json_type=1;
set enable_analyzer=1;
set output_format_native_write_json_as_string=0;
set input_format_json_infer_array_of_dynamic_from_array_of_different_types=0;

select '{"a" : false}'::JSON as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : null}'::JSON as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : 42}'::JSON as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : 42.42}'::JSON as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : [1, 2, 3]}'::JSON as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : {"b" : 42}}'::JSON as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : [{"b" : 42}]}'::JSON as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : [1, "str", [1]]}'::JSON as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');

select '{"a" : false}'::JSON(a Bool) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : null}'::JSON(a Nullable(UInt32)) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : 42}'::JSON(a Int64) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : 42}'::JSON(a UInt64) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : 42}'::JSON(a Int128) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : 42}'::JSON(a UInt128) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : 42}'::JSON(a Int256) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : 42}'::JSON(a UInt256) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : 42.42}'::JSON(a Float64) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : 42.42}'::JSON(a Decimal32(2)) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : 42.42}'::JSON(a Decimal64(2)) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : 42.42}'::JSON(a Decimal128(2)) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : 42.42}'::JSON(a Decimal256(2)) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : "41b43b2a-46c9-4ff8-a354-621299fd5d52"}'::JSON(a UUID) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : "121.0.0.1"}'::JSON(a IPv4) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : "2001:0db8:85a3:0000:0000:8a2e:0370:7334"}'::JSON(a IPv6) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : [1, 2, 3]}'::JSON(a Array(Int64)) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : [1, "str", [1]]}'::JSON(a Tuple(Int64, String, Array(Int64))) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : {"b" : 1, "c" : "str", "d" : [1]}}'::JSON(a Tuple(b Int64, c String, d Array(Int64))) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : {"b" : 1, "c" : 2, "d" : 3}}'::JSON(a Map(String, Int64)) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : [{"b" : 42}]}'::JSON(a Array(JSON(b Int64))) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');

select '{"a" : false}'::JSON(max_dynamic_paths=0) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : null}'::JSON(max_dynamic_paths=0) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : 42}'::JSON(max_dynamic_paths=0) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : 42.42}'::JSON(max_dynamic_paths=0) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : [1, 2, 3]}'::JSON(max_dynamic_paths=0) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : {"b" : 42}}'::JSON(max_dynamic_paths=0) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : [{"b" : 42}]}'::JSON(max_dynamic_paths=0) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : [1, "str", [1]]}'::JSON(max_dynamic_paths=0) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');

select '{"a" : false}'::JSON(max_dynamic_types=0) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : null}'::JSON(max_dynamic_types=0) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : 42}'::JSON(max_dynamic_types=0) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : 42.42}'::JSON(max_dynamic_types=0) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : [1, 2, 3]}'::JSON(max_dynamic_types=0) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : {"b" : 42}}'::JSON(max_dynamic_types=0) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : [{"b" : 42}]}'::JSON(max_dynamic_types=0) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
select '{"a" : [1, "str", [1]]}'::JSON(max_dynamic_types=0) as json, JSONAllPathsWithTypes(json) from remote('127.0.0.2', 'system.one');
