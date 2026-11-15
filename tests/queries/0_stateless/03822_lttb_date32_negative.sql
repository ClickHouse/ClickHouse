-- Test that largestTriangleThreeBuckets works correctly with Date32 values before Unix epoch
SELECT largestTriangleThreeBuckets(1)(0, '1900-01-01'::Date32);
