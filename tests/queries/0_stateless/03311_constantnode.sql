SELECT uniqUpTo(5)(CAST(unhex('00'), 'AggregateFunction(uniqUpTo(5), Nullable(Nothing))'));
SELECT largestTriangleThreeBucketsMerge(4)(CAST(unhex('0101000000000000F03F000000000000F03F'), 'AggregateFunction(largestTriangleThreeBuckets(4), UInt8 ,UInt8)'));
