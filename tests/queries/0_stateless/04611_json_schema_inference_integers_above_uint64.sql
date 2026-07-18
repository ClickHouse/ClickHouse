-- Test for issue #110563: JSONEachRow schema inference failed with a parsing error
-- for valid JSON integer literals that don't fit into Int64/UInt64.
-- Such integers are now inferred as Float64, the same way as oversized integers
-- inside strings (see tryInferNumberFromStringImpl).

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
select 'huge integer in Values format';
desc format('Values', '(2942420318599003496251392)');
desc format('Values', '(1), (2942420318599003496251392)');
