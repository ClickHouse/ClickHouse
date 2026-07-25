-- An injective cast on a key column must keep the excluded range bounds, a lossy one must not
SET optimize_use_implicit_projections = 1;

SELECT 'wide integer cast on the leading key column';
DROP TABLE IF EXISTS t_cast_asc;
CREATE TABLE t_cast_asc (g UInt32, r UInt32) ENGINE = MergeTree ORDER BY (g, r) SETTINGS index_granularity = 4;
INSERT INTO t_cast_asc SELECT number % 10, 1000 - number FROM numbers(1000);
SELECT count() FROM t_cast_asc WHERE g = materialize(toUInt256(5)) AND r = 55 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_cast_asc WHERE g = materialize(toUInt256(5)) AND r = 55 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT countIf(g = 5 AND r = 55) FROM t_cast_asc;
SELECT substring(explain, position(explain, 'Granules:')) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cast_asc WHERE g = materialize(toUInt256(5)) AND r = 55 SETTINGS use_lightweight_primary_key_index_analysis = 1) WHERE explain LIKE '%Granules: %/%';
SELECT count() FROM t_cast_asc WHERE toUInt256(g) = 5 AND r = 55;
SELECT substring(explain, position(explain, 'Granules:')) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cast_asc WHERE toUInt256(g) = 5 AND r = 55) WHERE explain LIKE '%Granules: %/%';
SELECT count() FROM t_cast_asc WHERE toUInt8(g) = 5 AND r = 55;
SELECT countIf(toUInt8(g) = 5 AND r = 55) FROM t_cast_asc;
DROP TABLE t_cast_asc;

SELECT 'the same on a reverse sorting key';
DROP TABLE IF EXISTS t_cast_desc;
CREATE TABLE t_cast_desc (g UInt32, r UInt32) ENGINE = MergeTree ORDER BY (g, r DESC) SETTINGS index_granularity = 4;
INSERT INTO t_cast_desc SELECT number % 10, 1000 - number FROM numbers(1000);
SELECT count() FROM t_cast_desc WHERE g = materialize(toUInt256(5)) AND r = 945 SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_cast_desc WHERE g = materialize(toUInt256(5)) AND r = 945 SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT countIf(g = 5 AND r = 945) FROM t_cast_desc;
SELECT substring(explain, position(explain, 'Granules:')) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cast_desc WHERE g = materialize(toUInt256(5)) AND r = 945 SETTINGS use_lightweight_primary_key_index_analysis = 1) WHERE explain LIKE '%Granules: %/%';
DROP TABLE t_cast_desc;

SELECT 'three key columns, single-row granules';
DROP TABLE IF EXISTS t_cast_pk;
CREATE TABLE t_cast_pk (x UInt64, y UInt64, z UInt64) ENGINE = MergeTree ORDER BY (x, y, z) SETTINGS index_granularity = 1;
INSERT INTO t_cast_pk VALUES (1, 11, 1235), (1, 11, 4395), (1, 22, 3545), (1, 22, 6984), (1, 33, 4596), (2, 11, 3572), (2, 11, 4563), (2, 11, 4578), (2, 22, 2791), (2, 22, 2791), (2, 22, 5786), (2, 22, 5786), (3, 33, 1235), (3, 33, 2791), (3, 33, 2791), (3, 44, 4578), (3, 44, 4935), (3, 55, 1235), (3, 55, 2791), (3, 55, 5786);
SELECT count() FROM t_cast_pk WHERE x = toUInt256(3) AND y = 55 AND 5786 >= z;
SELECT countIf(x = 3 AND y = 55 AND 5786 >= z) FROM t_cast_pk;
SELECT substring(explain, position(explain, 'Granules:')) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_cast_pk WHERE x = toUInt256(3) AND y = 55 AND 5786 >= z) WHERE explain LIKE '%Granules: %/%';
DROP TABLE t_cast_pk;

SELECT 'a chain that loses information keeps the widened bounds';
DROP TABLE IF EXISTS t_dt64;
CREATE TABLE t_dt64 (dt DateTime64(9), u UInt32) ENGINE = MergeTree ORDER BY (dt, u) SETTINGS index_granularity = 1;
INSERT INTO t_dt64 SELECT toDateTime64('2020-01-01 00:00:00', 9) + number / 1000000000, number % 3 FROM numbers(20);
SELECT count() FROM t_dt64 WHERE toDateTime64(dt, 3) = toDateTime64('2020-01-01 00:00:00', 3) AND u = 1;
SELECT countIf(toDateTime64(dt, 3) = toDateTime64('2020-01-01 00:00:00', 3) AND u = 1) FROM t_dt64;
DROP TABLE t_dt64;

SELECT 'a non-strict chain still needs the widened bounds';
DROP TABLE IF EXISTS t_non_strict;
CREATE TABLE t_non_strict (f Float64, u UInt32) ENGINE = MergeTree ORDER BY (f, u) SETTINGS index_granularity = 4;
INSERT INTO t_non_strict SELECT 1 + number / 100, number % 7 FROM numbers(1000);
SELECT count() FROM t_non_strict WHERE toUInt64(f) = 1 AND f >= 1.3 AND f <= 1.4 AND u > 0;
SELECT countIf(toUInt64(f) = 1 AND f >= 1.3 AND f <= 1.4 AND u > 0) FROM t_non_strict;
DROP TABLE t_non_strict;
