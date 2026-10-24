-- Index analysis of a decreasing function of a `Nullable` sorting key, e.g. `intDiv(a, -3)`. NULL is analysed
-- as `+inf` and the function maps it to NULL, so a granule of only NULLs must not match `intDiv(a, -3) = -2`,
-- a granule `[8, NULL]` must keep its NULLs for a NULL-matching `IN`, and the debug-build check of an exact
-- continuous range of marks must not fire, neither here nor for any NULL-matching set.

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;

DROP TABLE IF EXISTS t_null;
DROP TABLE IF EXISTS t_mixed;
DROP TABLE IF EXISTS t_decimal;
DROP TABLE IF EXISTS t_desc;
DROP TABLE IF EXISTS t_skip;

CREATE TABLE t_null (a Nullable(Int64)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 2, allow_nullable_key = 1;
INSERT INTO t_null SELECT NULL FROM numbers(10);

CREATE TABLE t_mixed (a Nullable(Int64)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 2, allow_nullable_key = 1;
INSERT INTO t_mixed SELECT if(number >= 9, NULL, number) FROM numbers(14);

SELECT 'all NULL';
SELECT (SELECT count() FROM t_null WHERE intDiv(a, -3) = -2) = (SELECT countIf(intDiv(a, -3) = -2) FROM t_null)
SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT (SELECT count() FROM t_null WHERE intDiv(a, -3) = -2) = (SELECT countIf(intDiv(a, -3) = -2) FROM t_null)
SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT (SELECT count() FROM t_null WHERE intDiv(a, -3) IN (-2)) = (SELECT countIf(intDiv(a, -3) IN (-2)) FROM t_null)
SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT (SELECT count() FROM t_null WHERE intDiv(a, -3) IN (-2)) = (SELECT countIf(intDiv(a, -3) IN (-2)) FROM t_null)
SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT (SELECT count() FROM t_null WHERE intDiv(a, -3) IN (NULL)) = (SELECT countIf(intDiv(a, -3) IN (NULL)) FROM t_null)
SETTINGS transform_null_in = 1, use_lightweight_primary_key_index_analysis = 0;
SELECT (SELECT count() FROM t_null WHERE intDiv(a, -3) IN (NULL)) = (SELECT countIf(intDiv(a, -3) IN (NULL)) FROM t_null)
SETTINGS transform_null_in = 1, use_lightweight_primary_key_index_analysis = 1;
SELECT (SELECT count() FROM t_null WHERE divide(a, -3) < 0) = (SELECT countIf(divide(a, -3) < 0) FROM t_null);
SELECT (SELECT count() FROM t_null WHERE intDiv(intDiv(a, -3), -2) = 1) = (SELECT countIf(intDiv(intDiv(a, -3), -2) = 1) FROM t_null);
SELECT count() FROM (EXPLAIN indexes = 1 SELECT sum(a) FROM t_null WHERE intDiv(a, -3) = -2) WHERE explain ILIKE '%Granules: 0/5%';

SELECT 'values and NULL';
SELECT (SELECT count() FROM t_mixed WHERE intDiv(a, -3) = -2) = (SELECT countIf(intDiv(a, -3) = -2) FROM t_mixed);
SELECT (SELECT count() FROM t_mixed WHERE intDiv(a, -3) IN (-2)) = (SELECT countIf(intDiv(a, -3) IN (-2)) FROM t_mixed);
SELECT (SELECT count() FROM t_mixed WHERE intDiv(a, -3) >= -2) = (SELECT countIf(intDiv(a, -3) >= -2) FROM t_mixed);
SELECT (SELECT count() FROM t_mixed WHERE intDiv(a, -3) < -2) = (SELECT countIf(intDiv(a, -3) < -2) FROM t_mixed);
SELECT (SELECT count() FROM t_mixed WHERE intDiv(a, -3) = 0) = (SELECT countIf(intDiv(a, -3) = 0) FROM t_mixed);
SELECT (SELECT count() FROM t_mixed WHERE divide(a, -3) > -2.5) = (SELECT countIf(divide(a, -3) > -2.5) FROM t_mixed);
SELECT count() FROM (EXPLAIN indexes = 1 SELECT sum(a) FROM t_mixed WHERE intDiv(a, -3) = -2) WHERE explain ILIKE '%Granules: 3/7%';
SELECT (SELECT count() FROM t_mixed WHERE intDiv(a, -3) IN (NULL)) = (SELECT countIf(intDiv(a, -3) IN (NULL)) FROM t_mixed)
SETTINGS transform_null_in = 1, use_lightweight_primary_key_index_analysis = 0;
SELECT (SELECT count() FROM t_mixed WHERE intDiv(a, -3) IN (NULL)) = (SELECT countIf(intDiv(a, -3) IN (NULL)) FROM t_mixed)
SETTINGS transform_null_in = 1, use_lightweight_primary_key_index_analysis = 1;

SELECT 'a function mapping NULL to a value';
SELECT (SELECT count() FROM t_null WHERE assumeNotNull(intDiv(a, -3)) = 0) = (SELECT countIf(assumeNotNull(intDiv(a, -3)) = 0) FROM t_null);
SELECT (SELECT count() FROM t_mixed WHERE assumeNotNull(intDiv(a, -3)) = 0) = (SELECT countIf(assumeNotNull(intDiv(a, -3)) = 0) FROM t_mixed);

SELECT 'Decimal key';
CREATE TABLE t_decimal (a Nullable(Decimal(18, 1))) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 2, allow_nullable_key = 1;
INSERT INTO t_decimal SELECT NULL FROM numbers(10);
SELECT (SELECT count() FROM t_decimal WHERE intDiv(a, -3) IN (-2)) = (SELECT countIf(intDiv(a, -3) IN (-2)) FROM t_decimal);

SELECT 'descending key';
CREATE TABLE t_desc (a Nullable(Int64)) ENGINE = MergeTree ORDER BY a DESC SETTINGS index_granularity = 2, allow_nullable_key = 1;
INSERT INTO t_desc SELECT if(number >= 9, NULL, number) FROM numbers(14);
SELECT (SELECT count() FROM t_desc WHERE intDiv(a, -3) = -2) = (SELECT countIf(intDiv(a, -3) = -2) FROM t_desc);
SELECT (SELECT count() FROM t_desc WHERE intDiv(a, -3) IN (-2)) = (SELECT countIf(intDiv(a, -3) IN (-2)) FROM t_desc);
SELECT (SELECT count() FROM t_desc WHERE intDiv(a, -3) = 0) = (SELECT countIf(intDiv(a, -3) = 0) FROM t_desc);

SELECT 'NULL-matching set over an increasing function or the bare key';
SELECT (SELECT count() FROM t_null WHERE a IN (NULL)) = (SELECT countIf(a IN (NULL)) FROM t_null)
SETTINGS transform_null_in = 1, use_lightweight_primary_key_index_analysis = 0;
SELECT (SELECT count() FROM t_mixed WHERE a IN (NULL)) = (SELECT countIf(a IN (NULL)) FROM t_mixed)
SETTINGS transform_null_in = 1, use_lightweight_primary_key_index_analysis = 1;
SELECT (SELECT count() FROM t_null WHERE intDiv(a, 3) IN (NULL)) = (SELECT countIf(intDiv(a, 3) IN (NULL)) FROM t_null)
SETTINGS transform_null_in = 1, use_lightweight_primary_key_index_analysis = 0;
SELECT (SELECT count() FROM t_null WHERE intDiv(a, 3) IN (NULL)) = (SELECT countIf(intDiv(a, 3) IN (NULL)) FROM t_null)
SETTINGS transform_null_in = 1, use_lightweight_primary_key_index_analysis = 1;
SELECT (SELECT count() FROM t_mixed WHERE intDiv(a, 3) IN (NULL)) = (SELECT countIf(intDiv(a, 3) IN (NULL)) FROM t_mixed)
SETTINGS transform_null_in = 1, use_lightweight_primary_key_index_analysis = 0;
SELECT (SELECT count() FROM t_mixed WHERE intDiv(a, 3) IN (NULL)) = (SELECT countIf(intDiv(a, 3) IN (NULL)) FROM t_mixed)
SETTINGS transform_null_in = 1, use_lightweight_primary_key_index_analysis = 1;
SELECT (SELECT count() FROM t_desc WHERE intDiv(a, 3) IN (NULL)) = (SELECT countIf(intDiv(a, 3) IN (NULL)) FROM t_desc)
SETTINGS transform_null_in = 1;

SELECT 'minmax skip index and column statistics';
CREATE TABLE t_skip (k UInt64, a Nullable(Int64), INDEX i a TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 2;
INSERT INTO t_skip SELECT number, if(number < 5, number, NULL) FROM numbers(16);
SELECT (SELECT count() FROM t_skip WHERE intDiv(a, -3) IN (NULL)) = (SELECT countIf(intDiv(a, -3) IN (NULL)) FROM t_skip)
SETTINGS transform_null_in = 1, use_statistics_for_part_pruning = 0;
SELECT (SELECT count() FROM t_skip WHERE intDiv(a, -3) IN (NULL)) = (SELECT countIf(intDiv(a, -3) IN (NULL)) FROM t_skip)
SETTINGS transform_null_in = 1, use_skip_indexes = 0;

DROP TABLE t_null;
DROP TABLE t_mixed;
DROP TABLE t_decimal;
DROP TABLE t_desc;
DROP TABLE t_skip;
