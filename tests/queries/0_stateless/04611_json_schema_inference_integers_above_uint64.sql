-- Test for issue #110563: JSONEachRow schema inference failed with a parsing error
-- for valid JSON integer literals that don't fit into Int64/UInt64.
-- Such integers are now inferred as Float64 in JSON formats, the same way as
-- oversized integers inside strings (see `tryInferNumberFromStringImpl`) and as
-- CSV already does. Formats inferring from escaped text (e.g. TSV) are unchanged:
-- they keep falling back to String, preserving the digits exactly.

select 'huge positive integer';
desc format('JSONEachRow', '{"num":2942420318599003496251392,"name":"Matt"}');
select 'huge negative integer';
desc format('JSONEachRow', '{"num":-2942420318599003496251392}');
select 'boundary values that fit into 64-bit types keep integer types';
desc format('JSONEachRow', '{"num":9223372036854775807}');
desc format('JSONEachRow', '{"num":-9223372036854775808}');
desc format('JSONEachRow', '{"num":18446744073709551615}');
select 'boundary values just outside 64-bit ranges';
desc format('JSONEachRow', '{"num":18446744073709551616}');
desc format('JSONEachRow', '{"num":-9223372036854775809}');
select 'merging with regular integers';
desc format('JSONEachRow', '{"num":1}, {"num":2942420318599003496251392}');
desc format('JSONEachRow', '{"num":2942420318599003496251392}, {"num":1}');
select 'huge integer inside an array';
desc format('JSONEachRow', '{"arr":[1, 2942420318599003496251392]}');
select 'TSV still infers String for huge integers';
desc format('TSV', '2942420318599003496251392');
select 'huge integer in a file (non-string read buffer)';
set engine_file_truncate_on_insert = 1;
insert into function file(currentDatabase() || '_04611.jsonl', 'LineAsString', 'line String') values ('{"num":2942420318599003496251392}');
desc file(currentDatabase() || '_04611.jsonl', 'JSONEachRow');
select 'Dynamic stores huge JSON integers as Float64, consistent with CSV';
drop table if exists t_04611_dynamic;
create table t_04611_dynamic (d Dynamic) engine = Memory;
insert into t_04611_dynamic format JSONEachRow {"d":2942420318599003496251392};
select dynamicType(d) from t_04611_dynamic;
drop table t_04611_dynamic;
