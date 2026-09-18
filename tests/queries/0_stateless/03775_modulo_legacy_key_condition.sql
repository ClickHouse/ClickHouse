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

-- Regression: `modulo` and `moduloLegacy` used to compute identically, so a query predicate on
-- `modulo(...)` could safely be rewritten to `moduloLegacy(...)` to prune partitions. They no
-- longer agree for a signed dividend with an equal-or-wider unsigned divisor (`moduloLegacy`
-- keeps the historical, sometimes unsigned-computed remainder; see DivisionUtils.h), so that
-- rewrite is unsound here: `moduloLegacy`'s range for this type pair never includes a negative
-- value, and the rewrite used to conclude every partition was provably excluded, dropping the
-- row that the (correct) `modulo(...) = -1` predicate actually matches.
-- The divisor is a bare literal outside Int32's range (inferred as UInt32) rather than an
-- explicit `toUInt32(...)` cast: the cast form does not reach the vulnerable code path at all
-- (its AST does not match the partition key's own by name), so it would not exercise this bug.
DROP TABLE IF EXISTS t_modulo_legacy_mixed_sign_partition_key;

CREATE TABLE t_modulo_legacy_mixed_sign_partition_key
(
    c0 Int32
)
ENGINE = MergeTree
PARTITION BY (c0 % 4000000000)
ORDER BY c0;

INSERT INTO t_modulo_legacy_mixed_sign_partition_key
SELECT number - 15
FROM numbers(30);

SELECT count()
FROM t_modulo_legacy_mixed_sign_partition_key
WHERE c0 = -1;

SELECT count()
FROM t_modulo_legacy_mixed_sign_partition_key
WHERE modulo(c0, 4000000000) = -1;
