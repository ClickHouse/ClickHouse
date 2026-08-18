-- Parity of the PARTITIONED build path of `partitioned_hash` (bits > 0: route scatter, per-leaf
-- hash tables, leaf builds) against `hash` and `parallel_hash`, across the map-type matrix.
-- The build side is large enough that the partition plan chooses more than one leaf for every
-- non-fixed-size map type, which is asserted through ProfileEvents at the end.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
-- Automatic external join would route to SpillingHashJoin at plan time (the absolute setting
-- can also arrive through test-level randomization, so pin both).
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;
-- The ProfileEvents assertions below read this server's query_log; with parallel replicas the
-- join builds (and their events) can land on other replicas.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_probe;
DROP TABLE IF EXISTS t_build_small;
DROP TABLE IF EXISTS t_probe_small;

CREATE TABLE t_build ENGINE = MergeTree ORDER BY tuple() AS
SELECT
    number AS k64,
    number * 2 AS k64b,
    number * 3 + 1 AS k64c,
    number * 5 + 2 AS k64d,
    toUInt32(number) AS k32,
    toUInt16(number % 65536) AS k16,
    number % 100000 AS kdup,
    toString(number) AS ks,
    toFixedString(leftPad(toString(number), 16, '0'), 16) AS kfs,
    if(number % 10 = 0, NULL, number) AS knull,
    toLowCardinality(toString(number % 200000)) AS klc,
    number + 1000000000 AS v
FROM numbers(300000);

CREATE TABLE t_probe ENGINE = MergeTree ORDER BY tuple() AS
SELECT
    number AS k64,
    number * 2 AS k64b,
    number * 3 + 1 AS k64c,
    number * 5 + 2 AS k64d,
    toUInt32(number) AS k32,
    toUInt16(number % 65536) AS k16,
    number % 100000 AS kdup,
    toString(number) AS ks,
    toFixedString(leftPad(toString(number), 16, '0'), 16) AS kfs,
    if(number % 7 = 0, NULL, number) AS knull,
    toLowCardinality(toString(number % 200000)) AS klc,
    number + 2000000000 AS pv
FROM numbers(600000);

-- Small tables for the fixed-size map types (key8/key16 always build a single leaf; a large
-- probe would explode the duplicate-key join output).
CREATE TABLE t_build_small ENGINE = MergeTree ORDER BY tuple() AS
SELECT toUInt8(number % 256) AS k8, toUInt16(number % 4096) AS k16, number + 3000000000 AS v FROM numbers(4096);
CREATE TABLE t_probe_small ENGINE = MergeTree ORDER BY tuple() AS
SELECT toUInt8(number % 256) AS k8, toUInt16(number % 4096) AS k16, number + 4000000000 AS pv FROM numbers(8192);

-- The parity checks below cannot pass vacuously: the supported shape must plan the partitioned
-- algorithm at this size.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64
    SETTINGS join_algorithm = 'partitioned_hash'
) WHERE explain LIKE '%Algorithm: PartitionedHashJoin%';

SELECT 'uint64 inner';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p3case uint64 inner';

SELECT 'uint64 left';
SELECT count(), sum(cityHash64(ifNull(b.v, 0), p.pv)) FROM t_probe AS p LEFT JOIN t_build AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash', join_use_nulls = 1;
SELECT count(), sum(cityHash64(ifNull(b.v, 0), p.pv)) FROM t_probe AS p LEFT JOIN t_build AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash', join_use_nulls = 1;
SELECT count(), sum(cityHash64(ifNull(b.v, 0), p.pv)) FROM t_probe AS p LEFT JOIN t_build AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash', join_use_nulls = 1, log_comment = 'p3case uint64 left';

SELECT 'uint64 inner duplicates';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p3case uint64 inner duplicates';

SELECT 'uint32 inner';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.k32 = b.k32 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.k32 = b.k32 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.k32 = b.k32 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p3case uint32 inner';

SELECT 'keys64 composite inner';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.k32 = b.k32 AND p.k16 = b.k16 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.k32 = b.k32 AND p.k16 = b.k16 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.k32 = b.k32 AND p.k16 = b.k16 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p3case keys64 composite inner';

SELECT 'keys128 composite inner';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 AND p.k32 = b.k32 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 AND p.k32 = b.k32 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 AND p.k32 = b.k32 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p3case keys128 composite inner';

SELECT 'keys128 composite left';
SELECT count(), sum(cityHash64(ifNull(b.v, 0), p.pv)) FROM t_probe AS p LEFT JOIN t_build AS b ON p.k64 = b.k64 AND p.k32 = b.k32 SETTINGS join_algorithm = 'hash', join_use_nulls = 1;
SELECT count(), sum(cityHash64(ifNull(b.v, 0), p.pv)) FROM t_probe AS p LEFT JOIN t_build AS b ON p.k64 = b.k64 AND p.k32 = b.k32 SETTINGS join_algorithm = 'parallel_hash', join_use_nulls = 1;
SELECT count(), sum(cityHash64(ifNull(b.v, 0), p.pv)) FROM t_probe AS p LEFT JOIN t_build AS b ON p.k64 = b.k64 AND p.k32 = b.k32 SETTINGS join_algorithm = 'partitioned_hash', join_use_nulls = 1, log_comment = 'p3case keys128 composite left';

SELECT 'keys256 composite inner';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 AND p.k64b = b.k64b AND p.k64c = b.k64c AND p.k32 = b.k32 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 AND p.k64b = b.k64b AND p.k64c = b.k64c AND p.k32 = b.k32 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 AND p.k64b = b.k64b AND p.k64c = b.k64c AND p.k32 = b.k32 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p3case keys256 composite inner';

SELECT 'hashed fixed inner';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 AND p.k64b = b.k64b AND p.k64c = b.k64c AND p.k64d = b.k64d AND p.k32 = b.k32 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 AND p.k64b = b.k64b AND p.k64c = b.k64c AND p.k64d = b.k64d AND p.k32 = b.k32 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 AND p.k64b = b.k64b AND p.k64c = b.k64c AND p.k64d = b.k64d AND p.k32 = b.k32 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p3case hashed fixed inner';

SELECT 'string inner';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p3case string inner';

SELECT 'string left';
SELECT count(), sum(cityHash64(ifNull(b.v, 0), p.pv)) FROM t_probe AS p LEFT JOIN t_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash', join_use_nulls = 1;
SELECT count(), sum(cityHash64(ifNull(b.v, 0), p.pv)) FROM t_probe AS p LEFT JOIN t_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'parallel_hash', join_use_nulls = 1;
SELECT count(), sum(cityHash64(ifNull(b.v, 0), p.pv)) FROM t_probe AS p LEFT JOIN t_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash', join_use_nulls = 1, log_comment = 'p3case string left';

SELECT 'fixedstring inner';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.kfs = b.kfs SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.kfs = b.kfs SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.kfs = b.kfs SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p3case fixedstring inner';

SELECT 'nullable inner';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.knull = b.knull SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.knull = b.knull SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.knull = b.knull SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p3case nullable inner';

SELECT 'nullable left';
SELECT count(), sum(cityHash64(ifNull(b.v, 0), p.pv)) FROM t_probe AS p LEFT JOIN t_build AS b ON p.knull = b.knull SETTINGS join_algorithm = 'hash', join_use_nulls = 1;
SELECT count(), sum(cityHash64(ifNull(b.v, 0), p.pv)) FROM t_probe AS p LEFT JOIN t_build AS b ON p.knull = b.knull SETTINGS join_algorithm = 'parallel_hash', join_use_nulls = 1;
SELECT count(), sum(cityHash64(ifNull(b.v, 0), p.pv)) FROM t_probe AS p LEFT JOIN t_build AS b ON p.knull = b.knull SETTINGS join_algorithm = 'partitioned_hash', join_use_nulls = 1, log_comment = 'p3case nullable left';

SELECT 'lowcardinality inner';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.klc = b.klc SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.klc = b.klc SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.klc = b.klc SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p3case lowcardinality inner';

SELECT 'hashed generic inner';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.ks = b.ks AND p.k64 = b.k64 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.ks = b.ks AND p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe AS p INNER JOIN t_build AS b ON p.ks = b.ks AND p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p3case hashed generic inner';

SELECT 'uint8 inner (single leaf)';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe_small AS p INNER JOIN t_build_small AS b ON p.k8 = b.k8 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe_small AS p INNER JOIN t_build_small AS b ON p.k8 = b.k8 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe_small AS p INNER JOIN t_build_small AS b ON p.k8 = b.k8 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p3single uint8 inner';

SELECT 'uint16 inner (single leaf)';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe_small AS p INNER JOIN t_build_small AS b ON p.k16 = b.k16 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe_small AS p INNER JOIN t_build_small AS b ON p.k16 = b.k16 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(b.v, p.pv)) FROM t_probe_small AS p INNER JOIN t_build_small AS b ON p.k16 = b.k16 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p3single uint16 inner';

-- Engagement and partitioning assertions: every `p3case` build must have chosen more than one
-- leaf, built the tables with a contiguous allocation and no heap fallbacks; the fixed-size map
-- types (`p3single`) must degenerate to exactly one leaf.
SYSTEM FLUSH LOGS query_log;
SELECT 'partition plans';
SELECT
    log_comment,
    ProfileEvents['PartitionedHashJoinPartitions'] > 1,
    ProfileEvents['PartitionedHashJoinLeafRows'] > 0,
    ProfileEvents['PartitionedHashJoinHashTableBytes'] > 0,
    ProfileEvents['PartitionedHashJoinHashTableGrowths']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'p3case %'
ORDER BY log_comment;
SELECT
    log_comment,
    ProfileEvents['PartitionedHashJoinPartitions'],
    ProfileEvents['PartitionedHashJoinLeafRows'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'p3single %'
ORDER BY log_comment;

DROP TABLE t_build;
DROP TABLE t_probe;
DROP TABLE t_build_small;
DROP TABLE t_probe_small;
