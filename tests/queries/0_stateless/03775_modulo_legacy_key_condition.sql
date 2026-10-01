-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.

SET explain_query_plan_default = 'legacy';

-- { echo }

DROP TABLE IF EXISTS t_modulo_legacy_partition_key;

CREATE TABLE t_modulo_legacy_partition_key
(
    x UInt64
)
ENGINE = MergeTree
PARTITION BY moduloLegacy(x, 16)
ORDER BY x
SETTINGS index_granularity = 1;

INSERT INTO t_modulo_legacy_partition_key
SELECT number
FROM numbers(20);

SELECT arraySort(groupArray(x))
FROM t_modulo_legacy_partition_key
WHERE x % 16 = 3;

EXPLAIN indexes = 1
SELECT arraySort(groupArray(x))
FROM t_modulo_legacy_partition_key
WHERE x % 16 = 3;

SELECT arraySort(groupArray(x))
FROM t_modulo_legacy_partition_key
WHERE x % 16 IN [3, 2];

EXPLAIN indexes = 1
SELECT arraySort(groupArray(x))
FROM t_modulo_legacy_partition_key
WHERE x % 16 IN [3, 2];

DROP TABLE IF EXISTS t_modulo_legacy_primary_key;

CREATE TABLE t_modulo_legacy_primary_key
(
    x UInt64
)
ENGINE = MergeTree
ORDER BY moduloLegacy(x, 16)
SETTINGS index_granularity = 1;

INSERT INTO t_modulo_legacy_primary_key
SELECT number
FROM numbers(20);

SELECT arraySort(groupArray(x))
FROM t_modulo_legacy_primary_key
WHERE x % 16 = 3;

EXPLAIN indexes = 1
SELECT arraySort(groupArray(x))
FROM t_modulo_legacy_primary_key
WHERE x % 16 = 3;

SELECT arraySort(groupArray(x))
FROM t_modulo_legacy_primary_key
WHERE x % 16 IN [3, 2];

EXPLAIN indexes = 1
SELECT arraySort(groupArray(x))
FROM t_modulo_legacy_primary_key
WHERE x % 16 IN [3, 2];

-- A partition key stores `moduloLegacy`, which is not `modulo` for every pair of argument types: with one
-- signed and one unsigned operand it applies the C++ `%`, which converts a negative dividend to unsigned when
-- the unsigned operand is as wide (`Int32 % UInt32`), and it casts the result to the divisor's width, so
-- `Int32 % 200` stores `-199` as `57`. A predicate on `modulo` must not be matched against such a key.
-- The divisors are bare literals: the type is inferred, and the name matches the one in the key.
DROP TABLE IF EXISTS t_modulo_legacy_unsigned_divisor;
DROP TABLE IF EXISTS t_modulo_legacy_wide_divisor;
DROP TABLE IF EXISTS t_modulo_legacy_narrow_result;
DROP TABLE IF EXISTS t_modulo_legacy_narrow_result_edge;
DROP TABLE IF EXISTS t_modulo_legacy_pruned;

CREATE TABLE t_modulo_legacy_unsigned_divisor (c0 Int32) ENGINE = MergeTree PARTITION BY (c0 % 4000000000) ORDER BY c0;
INSERT INTO t_modulo_legacy_unsigned_divisor SELECT number - 15 FROM numbers(30);

-- The row `-1` is there, and so are `-1` and `-2` in the set.
SELECT count() FROM t_modulo_legacy_unsigned_divisor WHERE c0 % 4000000000 = -1;
SELECT count() FROM t_modulo_legacy_unsigned_divisor WHERE c0 % 4000000000 IN (-1, -2);
SELECT count() FROM t_modulo_legacy_unsigned_divisor WHERE c0 % 4000000000 != -1;
SELECT count() FROM t_modulo_legacy_unsigned_divisor WHERE c0 % 4000000000 NOT IN (-1, -2);
-- Not a use of the key: control, the same rows by the column itself.
SELECT count() FROM t_modulo_legacy_unsigned_divisor WHERE c0 = -1;

CREATE TABLE t_modulo_legacy_wide_divisor (c0 Int64) ENGINE = MergeTree PARTITION BY (c0 % 10000000000000000000) ORDER BY c0;
INSERT INTO t_modulo_legacy_wide_divisor SELECT number - 15 FROM numbers(30);
SELECT count() FROM t_modulo_legacy_wide_divisor WHERE c0 % 10000000000000000000 = -1;
SELECT count() FROM t_modulo_legacy_wide_divisor WHERE c0 % 10000000000000000000 IN (-1, -2);

-- The result is narrowed to the divisor's width: `-199 % 200` is stored as `57`, and `128 % 129` as `-128`.
CREATE TABLE t_modulo_legacy_narrow_result (c0 Int32) ENGINE = MergeTree PARTITION BY (c0 % 200) ORDER BY c0;
INSERT INTO t_modulo_legacy_narrow_result VALUES (-199), (57), (5), (-5);
SELECT count() FROM t_modulo_legacy_narrow_result WHERE c0 % 200 = -199;
SELECT count() FROM t_modulo_legacy_narrow_result WHERE c0 % 200 = 57;
SELECT count() FROM t_modulo_legacy_narrow_result WHERE c0 % 200 IN (-199, 57);

CREATE TABLE t_modulo_legacy_narrow_result_edge (c0 Int32) ENGINE = MergeTree PARTITION BY (c0 % 129) ORDER BY c0;
INSERT INTO t_modulo_legacy_narrow_result_edge VALUES (128), (-128), (257), (-1);
SELECT count() FROM t_modulo_legacy_narrow_result_edge WHERE c0 % 129 = 128;
SELECT count() FROM t_modulo_legacy_narrow_result_edge WHERE c0 % 129 != -128;

-- Where the two agree the partition is still found: a signed dividend and a small constant unsigned divisor.
CREATE TABLE t_modulo_legacy_pruned (c0 Int32) ENGINE = MergeTree PARTITION BY (c0 % 16) ORDER BY c0 SETTINGS index_granularity = 1;
INSERT INTO t_modulo_legacy_pruned SELECT number - 100 FROM numbers(200);
SELECT count() FROM t_modulo_legacy_pruned WHERE c0 % 16 = -3;

EXPLAIN indexes = 1
SELECT count() FROM t_modulo_legacy_pruned WHERE c0 % 16 = -3;

DROP TABLE t_modulo_legacy_unsigned_divisor;
DROP TABLE t_modulo_legacy_wide_divisor;
DROP TABLE t_modulo_legacy_narrow_result;
DROP TABLE t_modulo_legacy_narrow_result_edge;
DROP TABLE t_modulo_legacy_pruned;
