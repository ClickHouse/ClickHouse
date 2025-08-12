SELECT uniqUpTo(5)(CAST(unhex('00'), 'AggregateFunction(uniqUpTo(5), Nullable(Nothing))'));
SELECT largestTriangleThreeBucketsMerge(4)(CAST(unhex('0101000000000000F03F000000000000F03F'), 'AggregateFunction(largestTriangleThreeBuckets(4), UInt8 ,UInt8)'));
SELECT map(('{"a":1,"b":1,"b":1}',),1, ('{"a":1}',),2, ('{"a":1,"c":1}',),2666514966)::Map(Tuple(JSON(max_dynamic_paths=2)),Variant(UInt32)) SETTINGS enable_variant_type = 1, enable_json_type = 1, type_json_skip_duplicated_paths = 1;
SELECT materialize(100000000) AND 0, arrayFold((acc, x) -> x, [0, 1], toUInt8(0));
DESCRIBE TABLE format(((toLowCardinality(0) OR NULL) OR inf OR 2) OR greatCircleAngle(materialize(0), 45, 1, 45) OR greatCircleAngle(toNullable(toUInt256(0)), 45, 1, 45) OR 0, JSONEachRow, toFixedString('\n{"a": "Hello", "b": 111}\n{"a": "World", "b": 123}\n{"a": "Hello", "b": 111}\n{"a": "World", "b": 123}\n', materialize(101))); -- { serverError ILLEGAL_COLUMN }
