-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.

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
