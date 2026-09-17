-- Result parity of `partitioned_hash` with `hash` and `parallel_hash` when the build is partitioned
-- (more than one partition of the shared hash table), for every hash-table key type. The build side
-- is large enough that every key type except the fixed-size ones (`UInt8`, `UInt16`) gets more than
-- one partition. The query log check at the end asserts that.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
-- The runner randomizes `max_bytes_before_external_join`; any non-zero spill budget would send the join
-- to `hash` at plan time.
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

-- Small tables for the `UInt8` and `UInt16` keys: their direct-index tables are never partitioned, and
-- with a large probe the duplicate keys would multiply the output.
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

SELECT 'uint64 inner', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04927 uint64 inner';

SELECT 'uint64 left', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(ifNull(b.v, 0), p.pv))) FROM t_probe AS p LEFT JOIN t_build AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash', join_use_nulls = 1) AS h,
    (SELECT (count(), sum(cityHash64(ifNull(b.v, 0), p.pv))) FROM t_probe AS p LEFT JOIN t_build AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash', join_use_nulls = 1) AS ph,
    (SELECT (count(), sum(cityHash64(ifNull(b.v, 0), p.pv))) FROM t_probe AS p LEFT JOIN t_build AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash', join_use_nulls = 1) AS pa)
SETTINGS log_comment = '04927 uint64 left';

SELECT 'uint64 inner duplicates', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04927 uint64 inner duplicates';

SELECT 'uint32 inner', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.k32 = b.k32 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.k32 = b.k32 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.k32 = b.k32 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04927 uint32 inner';

SELECT 'keys64 composite inner', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.k32 = b.k32 AND p.k16 = b.k16 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.k32 = b.k32 AND p.k16 = b.k16 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.k32 = b.k32 AND p.k16 = b.k16 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04927 keys64 composite inner';

SELECT 'keys128 composite inner', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 AND p.k32 = b.k32 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 AND p.k32 = b.k32 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 AND p.k32 = b.k32 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04927 keys128 composite inner';

SELECT 'keys128 composite left', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(ifNull(b.v, 0), p.pv))) FROM t_probe AS p LEFT JOIN t_build AS b ON p.k64 = b.k64 AND p.k32 = b.k32 SETTINGS join_algorithm = 'hash', join_use_nulls = 1) AS h,
    (SELECT (count(), sum(cityHash64(ifNull(b.v, 0), p.pv))) FROM t_probe AS p LEFT JOIN t_build AS b ON p.k64 = b.k64 AND p.k32 = b.k32 SETTINGS join_algorithm = 'parallel_hash', join_use_nulls = 1) AS ph,
    (SELECT (count(), sum(cityHash64(ifNull(b.v, 0), p.pv))) FROM t_probe AS p LEFT JOIN t_build AS b ON p.k64 = b.k64 AND p.k32 = b.k32 SETTINGS join_algorithm = 'partitioned_hash', join_use_nulls = 1) AS pa)
SETTINGS log_comment = '04927 keys128 composite left';

SELECT 'keys256 composite inner', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 AND p.k64b = b.k64b AND p.k64c = b.k64c AND p.k32 = b.k32 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 AND p.k64b = b.k64b AND p.k64c = b.k64c AND p.k32 = b.k32 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 AND p.k64b = b.k64b AND p.k64c = b.k64c AND p.k32 = b.k32 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04927 keys256 composite inner';

SELECT 'hashed fixed inner', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 AND p.k64b = b.k64b AND p.k64c = b.k64c AND p.k64d = b.k64d AND p.k32 = b.k32 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 AND p.k64b = b.k64b AND p.k64c = b.k64c AND p.k64d = b.k64d AND p.k32 = b.k32 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.k64 = b.k64 AND p.k64b = b.k64b AND p.k64c = b.k64c AND p.k64d = b.k64d AND p.k32 = b.k32 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04927 hashed fixed inner';

SELECT 'string inner', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04927 string inner';

SELECT 'string left', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(ifNull(b.v, 0), p.pv))) FROM t_probe AS p LEFT JOIN t_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash', join_use_nulls = 1) AS h,
    (SELECT (count(), sum(cityHash64(ifNull(b.v, 0), p.pv))) FROM t_probe AS p LEFT JOIN t_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'parallel_hash', join_use_nulls = 1) AS ph,
    (SELECT (count(), sum(cityHash64(ifNull(b.v, 0), p.pv))) FROM t_probe AS p LEFT JOIN t_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash', join_use_nulls = 1) AS pa)
SETTINGS log_comment = '04927 string left';

SELECT 'fixedstring inner', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.kfs = b.kfs SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.kfs = b.kfs SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.kfs = b.kfs SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04927 fixedstring inner';

SELECT 'nullable inner', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.knull = b.knull SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.knull = b.knull SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.knull = b.knull SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04927 nullable inner';

SELECT 'nullable left', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(ifNull(b.v, 0), p.pv))) FROM t_probe AS p LEFT JOIN t_build AS b ON p.knull = b.knull SETTINGS join_algorithm = 'hash', join_use_nulls = 1) AS h,
    (SELECT (count(), sum(cityHash64(ifNull(b.v, 0), p.pv))) FROM t_probe AS p LEFT JOIN t_build AS b ON p.knull = b.knull SETTINGS join_algorithm = 'parallel_hash', join_use_nulls = 1) AS ph,
    (SELECT (count(), sum(cityHash64(ifNull(b.v, 0), p.pv))) FROM t_probe AS p LEFT JOIN t_build AS b ON p.knull = b.knull SETTINGS join_algorithm = 'partitioned_hash', join_use_nulls = 1) AS pa)
SETTINGS log_comment = '04927 nullable left';

SELECT 'lowcardinality inner', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.klc = b.klc SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.klc = b.klc SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.klc = b.klc SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04927 lowcardinality inner';

SELECT 'hashed generic inner', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.ks = b.ks AND p.k64 = b.k64 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.ks = b.ks AND p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe AS p INNER JOIN t_build AS b ON p.ks = b.ks AND p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04927 hashed generic inner';

SELECT 'uint8 inner (one partition)', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe_small AS p INNER JOIN t_build_small AS b ON p.k8 = b.k8 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe_small AS p INNER JOIN t_build_small AS b ON p.k8 = b.k8 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe_small AS p INNER JOIN t_build_small AS b ON p.k8 = b.k8 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04927 single uint8 inner';

SELECT 'uint16 inner (one partition)', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe_small AS p INNER JOIN t_build_small AS b ON p.k16 = b.k16 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe_small AS p INNER JOIN t_build_small AS b ON p.k16 = b.k16 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(b.v, p.pv))) FROM t_probe_small AS p INNER JOIN t_build_small AS b ON p.k16 = b.k16 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04927 single uint16 inner';

-- Every build tagged `04927 <case>` must have used more than one partition and a non-empty hash table.
-- The `UInt8`/`UInt16` builds (`04927 single ...`) must have used exactly one.
SYSTEM FLUSH LOGS query_log;

SELECT 'partition plans';
SELECT
    log_comment,
    ProfileEvents['HashJoinPartitions'] > 1,
    ProfileEvents['HashJoinInsertedRows'] > 0,
    ProfileEvents['HashJoinTableBytes'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '04927 %' AND log_comment NOT LIKE '04927 single %'
ORDER BY log_comment;
SELECT
    log_comment,
    ProfileEvents['HashJoinPartitions'],
    ProfileEvents['HashJoinInsertedRows'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '04927 single %'
ORDER BY log_comment;

DROP TABLE t_build;
DROP TABLE t_probe;
DROP TABLE t_build_small;
DROP TABLE t_probe_small;
