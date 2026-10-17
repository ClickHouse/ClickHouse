-- `IN` pushes the set through the same key transform as `=`, but unlike `=` it matches float values bit-exactly:
-- `-0.0 IN (0.0)` is 0. The transformed set `{'0'}` therefore selects exactly the rows `f IN (0.0)` matches, and
-- the index keeps answering it; the results with and without the index must agree.
-- The `marks` estimates are compared against 4, the number of granules in the table, so that a value
-- below it means the index still prunes and 4 means a full scan.

DROP TABLE IF EXISTS t_signed_zero_in;
CREATE TABLE t_signed_zero_in (f Float64) ENGINE = MergeTree ORDER BY toString(f) SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_in VALUES (-0.0), (0.5), (0.0), (2.0);

SELECT count(), (SELECT count() FROM t_signed_zero_in WHERE f IN (0.0) SETTINGS use_primary_key = 0) FROM t_signed_zero_in WHERE f IN (0.0);
SELECT count(), (SELECT count() FROM t_signed_zero_in WHERE f IN (-0.0) SETTINGS use_primary_key = 0) FROM t_signed_zero_in WHERE f IN (-0.0);
SELECT count(), (SELECT count() FROM t_signed_zero_in WHERE f IN (0, 2) SETTINGS use_primary_key = 0) FROM t_signed_zero_in WHERE f IN (0, 2);
SELECT count(), (SELECT count() FROM t_signed_zero_in WHERE f NOT IN (0.0) SETTINGS use_primary_key = 0) FROM t_signed_zero_in WHERE f NOT IN (0.0);
SELECT count(), (SELECT count() FROM t_signed_zero_in WHERE f NOT IN (0.0, 2.0) SETTINGS use_primary_key = 0) FROM t_signed_zero_in WHERE f NOT IN (0.0, 2.0);
SELECT count(), (SELECT marks < 4 FROM (EXPLAIN ESTIMATE SELECT count() FROM t_signed_zero_in WHERE f IN (0.0))) FROM t_signed_zero_in WHERE f IN (0.0);
SELECT count(), (SELECT marks < 4 FROM (EXPLAIN ESTIMATE SELECT count() FROM t_signed_zero_in WHERE f IN (0.5, 2.0))) FROM t_signed_zero_in WHERE f IN (0.5, 2.0);

-- A transform that maps both zeros to the same key value.
DROP TABLE IF EXISTS t_signed_zero_in_round;
CREATE TABLE t_signed_zero_in_round (f Float64) ENGINE = MergeTree ORDER BY toString(f + 0.0) SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_in_round VALUES (-0.0), (0.5), (0.0), (2.0);
SELECT count(), (SELECT count() FROM t_signed_zero_in_round WHERE f IN (0.0) SETTINGS use_primary_key = 0) FROM t_signed_zero_in_round WHERE f IN (0.0);

-- A signed zero inside an `Array` key: `[-0.0] = [0.0]` holds, but `toString` tells the two apart, so the index
-- must not answer `a = [0.0]`. An array constant without a zero is not affected and keeps its lookup.
DROP TABLE IF EXISTS t_signed_zero_array;
CREATE TABLE t_signed_zero_array (a Array(Float64)) ENGINE = MergeTree ORDER BY toString(a) SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_array VALUES ([-0.0]), ([0.5]), ([0.0]), ([2.0]);

SELECT count(), (SELECT count() FROM t_signed_zero_array WHERE a = [0.0] SETTINGS use_primary_key = 0) FROM t_signed_zero_array WHERE a = [0.0];
SELECT count(), (SELECT count() FROM t_signed_zero_array WHERE a != [0.0] SETTINGS use_primary_key = 0) FROM t_signed_zero_array WHERE a != [0.0];
SELECT count(), (SELECT count() FROM t_signed_zero_array WHERE a IN ([0.0]) SETTINGS use_primary_key = 0) FROM t_signed_zero_array WHERE a IN ([0.0]);
SELECT count(), (SELECT marks < 4 FROM (EXPLAIN ESTIMATE SELECT count() FROM t_signed_zero_array WHERE a = [2.0])) FROM t_signed_zero_array WHERE a = [2.0];

-- The same inside a `Map` value.
DROP TABLE IF EXISTS t_signed_zero_map;
CREATE TABLE t_signed_zero_map (m Map(String, Float64)) ENGINE = MergeTree ORDER BY toString(m) SETTINGS index_granularity = 1, allow_suspicious_primary_key = 1;
INSERT INTO t_signed_zero_map VALUES (map('a', -0.0)), (map('a', 0.5)), (map('a', 0.0)), (map('a', 2.0));

SELECT count(), (SELECT count() FROM t_signed_zero_map WHERE m = map('a', 0.0) SETTINGS use_primary_key = 0) FROM t_signed_zero_map WHERE m = map('a', 0.0);
SELECT count(), (SELECT count() FROM t_signed_zero_map WHERE m != map('a', 0.0) SETTINGS use_primary_key = 0) FROM t_signed_zero_map WHERE m != map('a', 0.0);
SELECT count(), (SELECT marks < 4 FROM (EXPLAIN ESTIMATE SELECT count() FROM t_signed_zero_map WHERE m = map('a', 2.0))) FROM t_signed_zero_map WHERE m = map('a', 2.0);

DROP TABLE t_signed_zero_in;
DROP TABLE t_signed_zero_in_round;
DROP TABLE t_signed_zero_array;
DROP TABLE t_signed_zero_map;
