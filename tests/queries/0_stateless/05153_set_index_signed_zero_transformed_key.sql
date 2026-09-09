-- A predicate on a floating point zero holds for both `-0.` and `0.`, so an `IN` set that contains one
-- spelling of a zero must not prune away the granule that holds the other one. That happens when the
-- sorting key is a deterministic transform which sends the two spellings to different key values.
-- The ground truth is taken with `countIf`, which evaluates the predicate on every row, because the
-- same composition on the `equals` atom is fixed separately.

DROP TABLE IF EXISTS t_signed_zero_reinterpret;
DROP TABLE IF EXISTS t_signed_zero_to_string;
DROP TABLE IF EXISTS t_signed_zero_float32;
DROP TABLE IF EXISTS t_signed_zero_nullable;
DROP TABLE IF EXISTS t_signed_zero_integer_key;

SELECT 'injective transform: the two spellings are two different key values';
CREATE TABLE t_signed_zero_reinterpret (f Float64) ENGINE = MergeTree ORDER BY reinterpretAsUInt64(f)
    SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_reinterpret VALUES (-0.), (1.), (2.);
SELECT count() FROM t_signed_zero_reinterpret WHERE f IN (0., 999.);
SELECT countIf(f IN (0., 999.)) FROM t_signed_zero_reinterpret;
SELECT count() FROM t_signed_zero_reinterpret WHERE f NOT IN (0.);
SELECT countIf(f NOT IN (0.)) FROM t_signed_zero_reinterpret;
SELECT count() FROM t_signed_zero_reinterpret WHERE f IN (1., 2.);

SELECT 'non-injective transform: the pruner only relaxes `can_be_false`';
CREATE TABLE t_signed_zero_to_string (f Float64) ENGINE = MergeTree ORDER BY toString(f)
    SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_to_string VALUES (-0.), (1.), (2.);
SELECT count() FROM t_signed_zero_to_string WHERE f IN (0., 999.);
SELECT countIf(f IN (0., 999.)) FROM t_signed_zero_to_string;
SELECT count() FROM t_signed_zero_to_string WHERE tuple(f) IN (tuple(0.));
SELECT countIf(tuple(f) IN (tuple(0.))) FROM t_signed_zero_to_string;

SELECT 'Float32';
CREATE TABLE t_signed_zero_float32 (f Float32) ENGINE = MergeTree ORDER BY reinterpretAsUInt32(f)
    SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_float32 VALUES (-0.), (1.);
SELECT count() FROM t_signed_zero_float32 WHERE f IN (0.::Float32);
SELECT countIf(f IN (0.::Float32)) FROM t_signed_zero_float32;

SELECT 'Nullable';
CREATE TABLE t_signed_zero_nullable (f Nullable(Float64)) ENGINE = MergeTree ORDER BY toString(f)
    SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO t_signed_zero_nullable VALUES (-0.), (1.), (NULL);
SELECT count() FROM t_signed_zero_nullable WHERE f IN (0.);
SELECT countIf(f IN (0.)) FROM t_signed_zero_nullable;

SELECT 'a transformed key over a domain without two spellings of a value still prunes';
CREATE TABLE t_signed_zero_integer_key (i Int64) ENGINE = MergeTree ORDER BY reinterpretAsUInt64(i)
    SETTINGS index_granularity = 1;
INSERT INTO t_signed_zero_integer_key SELECT number FROM numbers(1000);
SELECT count() FROM t_signed_zero_integer_key WHERE i IN (10, 20) SETTINGS max_rows_to_read = 8;

DROP TABLE t_signed_zero_reinterpret;
DROP TABLE t_signed_zero_to_string;
DROP TABLE t_signed_zero_float32;
DROP TABLE t_signed_zero_nullable;
DROP TABLE t_signed_zero_integer_key;
