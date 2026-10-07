-- `-0.0` and `+0.0` compare equal, but a key transform can tell them apart
-- (`toString(-0.0)` is '-0'), so the primary key index must not answer `f = 0.0` exactly.
-- The `marks` estimates are compared against 4, the number of granules in the table,
-- so that a value below it means the index still prunes and 4 means a full scan.

DROP TABLE IF EXISTS t_signed_zero_string;
CREATE TABLE t_signed_zero_string (f Float64) ENGINE = MergeTree ORDER BY toString(f) SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_string VALUES (-0.0), (-0.5), (0.0), (2.0);

SELECT count(), (SELECT count() FROM t_signed_zero_string WHERE f = 0.0 SETTINGS use_primary_key = 0) FROM t_signed_zero_string WHERE f = 0.0;
SELECT count(), (SELECT count() FROM t_signed_zero_string WHERE f = -0.0 SETTINGS use_primary_key = 0) FROM t_signed_zero_string WHERE f = -0.0;
-- An integer literal reaches the transform as `+0.0` and has the same problem.
SELECT count(), (SELECT count() FROM t_signed_zero_string WHERE f = 0 SETTINGS use_primary_key = 0) FROM t_signed_zero_string WHERE f = 0;
-- `notEquals` used to count the `-0.0` row without filtering it, because the key atom was treated as exact.
SELECT count(), (SELECT count() FROM t_signed_zero_string WHERE f != 0.0 SETTINGS use_primary_key = 0) FROM t_signed_zero_string WHERE f != 0.0;
-- A constant that is not a zero keeps its single-granule lookup.
SELECT count(), (SELECT marks < 4 FROM (EXPLAIN ESTIMATE SELECT count() FROM t_signed_zero_string WHERE f = 2.0)) FROM t_signed_zero_string WHERE f = 2.0;

DROP TABLE IF EXISTS t_signed_zero_reinterpret;
CREATE TABLE t_signed_zero_reinterpret (f Float64) ENGINE = MergeTree ORDER BY reinterpretAsUInt64(f) SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_reinterpret VALUES (-0.0), (-0.5), (0.0), (2.0);

SELECT count(), (SELECT count() FROM t_signed_zero_reinterpret WHERE f = 0.0 SETTINGS use_primary_key = 0) FROM t_signed_zero_reinterpret WHERE f = 0.0;

-- A non-injective transform maps `1.0` and `-0.0` to the same key value, so the atom for `f = 1.0`
-- is relaxed: the rows it returns are filtered afterwards, but the granules it skips must not hold
-- a matching row. A non-zero constant is unambiguous, so the pruning stays; a zero constant matches
-- both zeros, which this key does distinguish, so the index cannot answer it and the whole table
-- (9 granules) is read.
DROP TABLE IF EXISTS t_signed_zero_relaxed;
CREATE TABLE t_signed_zero_relaxed (f Float64) ENGINE = MergeTree ORDER BY notEquals(reinterpretAsUInt64(f), 0) SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_relaxed SELECT 0.0 FROM numbers(6);
INSERT INTO t_signed_zero_relaxed VALUES (-0.0), (1.0), (2.0);
OPTIMIZE TABLE t_signed_zero_relaxed FINAL;

SELECT count(), (SELECT marks < 9 FROM (EXPLAIN ESTIMATE SELECT count() FROM t_signed_zero_relaxed WHERE f = 1.0)) FROM t_signed_zero_relaxed WHERE f = 1.0;
SELECT count(), (SELECT count() FROM t_signed_zero_relaxed WHERE f = 0.0 SETTINGS use_primary_key = 0), (SELECT marks FROM (EXPLAIN ESTIMATE SELECT count() FROM t_signed_zero_relaxed WHERE f = 0.0)) FROM t_signed_zero_relaxed WHERE f = 0.0;

DROP TABLE t_signed_zero_string;
DROP TABLE t_signed_zero_reinterpret;
DROP TABLE t_signed_zero_relaxed;
