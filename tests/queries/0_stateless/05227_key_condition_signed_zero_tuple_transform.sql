-- A signed zero inside a `Tuple` key: `(-0.0, 1.0) = (0.0, 1.0)` holds, but `toString` tells the two
-- apart, so the primary key index must not answer `k = (0.0, 1.0)` at all - neither exactly nor as a
-- relaxed atom, because a relaxed atom would still skip the granule that holds `(-0.0, 1.0)`.
-- The `marks` estimates are compared against 4, the number of granules in the table, so that a value
-- below it means the index still prunes and 4 means a full scan.

DROP TABLE IF EXISTS t_signed_zero_tuple;
CREATE TABLE t_signed_zero_tuple (k Tuple(Float64, Float64)) ENGINE = MergeTree ORDER BY toString(k) SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_tuple VALUES ((-0.0, 1.0)), ((-0.5, 1.0)), ((0.0, 1.0)), ((2.0, 1.0));

SELECT count(), (SELECT count() FROM t_signed_zero_tuple WHERE k = (0.0, 1.0) SETTINGS use_primary_key = 0) FROM t_signed_zero_tuple WHERE k = (0.0, 1.0);
SELECT count(), (SELECT count() FROM t_signed_zero_tuple WHERE k = (-0.0, 1.0) SETTINGS use_primary_key = 0) FROM t_signed_zero_tuple WHERE k = (-0.0, 1.0);
-- An integer literal inside the tuple reaches the key as `+0.0` and has the same problem.
SELECT count(), (SELECT count() FROM t_signed_zero_tuple WHERE k = (0, 1) SETTINGS use_primary_key = 0) FROM t_signed_zero_tuple WHERE k = (0, 1);
SELECT count(), (SELECT count() FROM t_signed_zero_tuple WHERE k != (0.0, 1.0) SETTINGS use_primary_key = 0) FROM t_signed_zero_tuple WHERE k != (0.0, 1.0);
SELECT count(), (SELECT marks FROM (EXPLAIN ESTIMATE SELECT count() FROM t_signed_zero_tuple WHERE k = (0.0, 1.0))) FROM t_signed_zero_tuple WHERE k = (0.0, 1.0);
-- A tuple constant without a zero keeps its single-granule lookup.
SELECT count(), (SELECT marks < 4 FROM (EXPLAIN ESTIMATE SELECT count() FROM t_signed_zero_tuple WHERE k = (2.0, 1.0))) FROM t_signed_zero_tuple WHERE k = (2.0, 1.0);

-- A NaN inside the tuple equals nothing, not even itself, while the transformed key value `'(nan,1)'`
-- compares as equal: the index must not answer such a predicate either.
INSERT INTO t_signed_zero_tuple VALUES ((nan, 1.0));
SELECT count(), (SELECT count() FROM t_signed_zero_tuple WHERE k = (nan, 1.0) SETTINGS use_primary_key = 0) FROM t_signed_zero_tuple WHERE k = (nan, 1.0);
SELECT count(), (SELECT count() FROM t_signed_zero_tuple WHERE k != (nan, 1.0) SETTINGS use_primary_key = 0) FROM t_signed_zero_tuple WHERE k != (nan, 1.0);

-- The zero matters only at a float element: a zero in an integer element next to a non-zero float keeps pruning.
DROP TABLE IF EXISTS t_signed_zero_mixed_tuple;
CREATE TABLE t_signed_zero_mixed_tuple (k Tuple(UInt64, Float64)) ENGINE = MergeTree ORDER BY toString(k) SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_mixed_tuple VALUES ((0, -0.0)), ((0, 0.5)), ((0, 0.0)), ((1, 2.0));

SELECT count(), (SELECT marks < 4 FROM (EXPLAIN ESTIMATE SELECT count() FROM t_signed_zero_mixed_tuple WHERE k = (0, 0.5))) FROM t_signed_zero_mixed_tuple WHERE k = (0, 0.5);
SELECT count(), (SELECT count() FROM t_signed_zero_mixed_tuple WHERE k = (0, 0.0) SETTINGS use_primary_key = 0) FROM t_signed_zero_mixed_tuple WHERE k = (0, 0.0);
SELECT count(), (SELECT count() FROM t_signed_zero_mixed_tuple WHERE k != (0, 0.0) SETTINGS use_primary_key = 0) FROM t_signed_zero_mixed_tuple WHERE k != (0, 0.0);

DROP TABLE t_signed_zero_tuple;
DROP TABLE t_signed_zero_mixed_tuple;
