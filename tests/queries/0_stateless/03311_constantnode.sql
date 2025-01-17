SELECT uniqUpTo(5)(CAST(unhex('00'), 'AggregateFunction(uniqUpTo(5), Nullable(Nothing))'));
SELECT largestTriangleThreeBucketsMerge(4)(CAST(unhex('0101000000000000F03F000000000000F03F'), 'AggregateFunction(largestTriangleThreeBuckets(4), UInt8 ,UInt8)'));
SELECT map(('{"a":1,"b":1,"b":1}',),1, ('{"a":1}',),2, ('{"a":1,"c":1}',),2666514966)::Map(Tuple(JSON(max_dynamic_paths=2)),Variant(UInt32)) SETTINGS enable_variant_type = 1, allow_experimental_json_type = 1, type_json_skip_duplicated_paths = 1;
