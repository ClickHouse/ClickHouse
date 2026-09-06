-- `-0.0` and `+0.0` compare equal, but an injective key transform can tell them apart
-- (`toString(-0.0)` is '-0'), so the primary key index must not answer `f = 0.0` exactly.

DROP TABLE IF EXISTS t_signed_zero_string;
CREATE TABLE t_signed_zero_string (f Float64) ENGINE = MergeTree ORDER BY toString(f) SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_string VALUES (-0.0), (-0.5), (0.0), (2.0);

SELECT count(), (SELECT count() FROM t_signed_zero_string WHERE f = 0.0 SETTINGS use_primary_key = 0) FROM t_signed_zero_string WHERE f = 0.0;
SELECT count(), (SELECT count() FROM t_signed_zero_string WHERE f = -0.0 SETTINGS use_primary_key = 0) FROM t_signed_zero_string WHERE f = -0.0;
-- An integer literal reaches the transform as `+0.0` and has the same problem.
SELECT count(), (SELECT count() FROM t_signed_zero_string WHERE f = 0 SETTINGS use_primary_key = 0) FROM t_signed_zero_string WHERE f = 0;
-- `notEquals` used to count the `-0.0` row without filtering it, because the key atom was treated as exact.
SELECT count(), (SELECT count() FROM t_signed_zero_string WHERE f != 0.0 SETTINGS use_primary_key = 0) FROM t_signed_zero_string WHERE f != 0.0;
-- A constant that is not a zero keeps its exact single-granule lookup.
SELECT count(), (SELECT marks FROM (EXPLAIN ESTIMATE SELECT count() FROM t_signed_zero_string WHERE f = 2.0)) FROM t_signed_zero_string WHERE f = 2.0;

DROP TABLE IF EXISTS t_signed_zero_reinterpret;
CREATE TABLE t_signed_zero_reinterpret (f Float64) ENGINE = MergeTree ORDER BY reinterpretAsUInt64(f) SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_reinterpret VALUES (-0.0), (-0.5), (0.0), (2.0);

SELECT count(), (SELECT count() FROM t_signed_zero_reinterpret WHERE f = 0.0 SETTINGS use_primary_key = 0) FROM t_signed_zero_reinterpret WHERE f = 0.0;

DROP TABLE t_signed_zero_string;
DROP TABLE t_signed_zero_reinterpret;
