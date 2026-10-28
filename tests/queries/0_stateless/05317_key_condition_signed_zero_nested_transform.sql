-- A signed zero nested behind more than one container, such as `Array(Tuple(Float64))`: `[tuple(-0.0)] = [tuple(0.0)]` holds,
-- but `toString` tells the two apart, so the index must not answer `a = [tuple(0.0)]`. The results with and without the
-- index must agree. A constant without a zero is not affected and keeps its lookup; the `marks` estimate is compared
-- against 4, the number of granules in the table.

DROP TABLE IF EXISTS t_signed_zero_nested;
CREATE TABLE t_signed_zero_nested (a Array(Tuple(Float64))) ENGINE = MergeTree ORDER BY toString(a) SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_nested VALUES ([tuple(-0.0)]), ([tuple(0.5)]), ([tuple(0.0)]), ([tuple(2.0)]);

SELECT count(), (SELECT count() FROM t_signed_zero_nested WHERE a = [tuple(0.0)] SETTINGS use_primary_key = 0) FROM t_signed_zero_nested WHERE a = [tuple(0.0)];
SELECT count(), (SELECT count() FROM t_signed_zero_nested WHERE a != [tuple(0.0)] SETTINGS use_primary_key = 0) FROM t_signed_zero_nested WHERE a != [tuple(0.0)];
SELECT count(), (SELECT marks < 4 FROM (EXPLAIN ESTIMATE SELECT count() FROM t_signed_zero_nested WHERE a = [tuple(2.0)])) FROM t_signed_zero_nested WHERE a = [tuple(2.0)];

-- The same with the float behind `Nullable` inside a `Map` of `Array`.
DROP TABLE IF EXISTS t_signed_zero_nested_map;
CREATE TABLE t_signed_zero_nested_map (m Map(String, Array(Nullable(Float64)))) ENGINE = MergeTree ORDER BY toString(m) SETTINGS index_granularity = 1, allow_suspicious_primary_key = 1;
INSERT INTO t_signed_zero_nested_map VALUES (map('a', [-0.0])), (map('a', [0.5])), (map('a', [0.0])), (map('a', [2.0]));

SELECT count(), (SELECT count() FROM t_signed_zero_nested_map WHERE m = map('a', [0.0]) SETTINGS use_primary_key = 0) FROM t_signed_zero_nested_map WHERE m = map('a', [0.0]);
SELECT count(), (SELECT count() FROM t_signed_zero_nested_map WHERE m != map('a', [0.0]) SETTINGS use_primary_key = 0) FROM t_signed_zero_nested_map WHERE m != map('a', [0.0]);

DROP TABLE t_signed_zero_nested;
DROP TABLE t_signed_zero_nested_map;
