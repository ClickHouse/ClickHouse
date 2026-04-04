-- Test that topKWeighted accepts floating-point weights (issue #40702)

-- Float64 weight
SELECT topKWeighted(2)(k, w) FROM VALUES('k String, w Float64', ('y', 1.5), ('y', 1.5), ('x', 5.0), ('y', 1.5), ('z', 10.0));

-- Float32 weight
SELECT topKWeighted(2)(k, w) FROM VALUES('k String, w Float32', ('a', 1.0), ('b', 5.0), ('c', 10.0));

-- approx_top_sum also accepts float weights
SELECT approx_top_sum(2)(k, w) FROM VALUES('k String, w Float64', ('a', 1.0), ('b', 5.0), ('c', 10.0));

-- Integer weights still work
SELECT topKWeighted(2)(k, w) FROM VALUES('k String, w UInt64', ('a', 1), ('b', 5), ('c', 10));
