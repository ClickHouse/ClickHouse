-- Results of the partitioned hash join over a duplicate-heavy build. Keys carry 1, 2, 3, 7, 8, 9
-- and 300 rows each, spread over blocks. Every duplicate layout of the shared hash table is produced
-- and read back: inline, pair, run, and run list. Coverage includes join kinds and strictnesses, key
-- types, ON filters, USING, `join_use_nulls`, thread counts, and empty or all-miss inputs. Each line
-- prints the row count.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
-- The runner randomizes `max_bytes_before_external_join`; any non-zero spill budget would wrap the join
-- in `SpillingHashJoin`.
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;
-- The ProfileEvents assertions below read this server's query_log; with parallel replicas the join builds
-- (and their events) can land on other replicas.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_ir_build;
DROP TABLE IF EXISTS t_ir_probe;

-- 300000 keys; the residue of the key index picks its multiplicity. Rows are ordered by the duplicate
-- index first, so a key's copies sit in different blocks of the build stream.
CREATE TABLE t_ir_build ENGINE = MergeTree ORDER BY tuple() AS
SELECT
    i AS k,
    toString(i) AS ks,
    toFixedString(leftPad(toString(i), 16, '0'), 16) AS kfs,
    if(i % 11 = 0, NULL, toUInt32(i)) AS knull,
    toLowCardinality(toString(i % 50000)) AS klc,
    toUInt16(i % 50000) AS k16,
    toUInt8(i % 200) AS k8,
    toUInt32(i) AS k32,
    i * 7 AS k2,
    i * 13 AS k3,
    toUInt64(d) AS ts,
    i * 1000 + d AS v
FROM
(
    SELECT
        number AS i,
        multiIf(number % 100 < 60, 1, number % 100 < 80, 2, number % 100 < 90, 3, number % 100 < 95, 7, number % 100 < 98, 8, number % 100 = 98, 9, 300) AS m
    FROM numbers(300000)
)
ARRAY JOIN range(m) AS d
ORDER BY d, i;

CREATE TABLE t_ir_probe ENGINE = MergeTree ORDER BY tuple() AS
SELECT
    number AS k,
    toString(number) AS ks,
    toFixedString(leftPad(toString(number), 16, '0'), 16) AS kfs,
    if(number % 7 = 0, NULL, toUInt32(number)) AS knull,
    toString(number % 50000) AS klc,
    toUInt16(number % 50000) AS k16,
    toUInt8(number % 200) AS k8,
    toUInt32(number) AS k32,
    number * 7 AS k2,
    number * 13 AS k3,
    toUInt64(5) AS ts,
    number AS p
FROM numbers(450000);

SELECT 'hash is selected', count() > 0 FROM (EXPLAIN actions = 1 SELECT p.p FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') WHERE explain LIKE '%Algorithm: PartitionedHashJoin%';

SELECT 'inner all uint64', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa)
SETTINGS log_comment = '05111 inner all uint64';

SELECT 'left all uint64', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p LEFT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'right all uint64', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'full all uint64', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p FULL JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'full all uint64 join_use_nulls', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(ifNull(p.p, 0), ifNull(b.v, 0)))) FROM t_ir_probe AS p FULL JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', join_use_nulls = 1) AS pa);

-- ANY picks an arbitrary row per key, so only key-determined expressions are compared.
SELECT 'inner any uint64', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_ir_probe AS p ANY INNER JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'left any uint64', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_ir_probe AS p ANY LEFT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'right any uint64', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v))) FROM t_ir_probe AS p ANY RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'left semi uint64', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_ir_probe AS p SEMI LEFT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'left anti uint64', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p))) FROM t_ir_probe AS p ANTI LEFT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'right semi uint64', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v))) FROM t_ir_probe AS p SEMI RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'right anti uint64', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v))) FROM (SELECT * FROM t_ir_probe WHERE k % 2 = 0) AS p ANTI RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'inner all string', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'right all string', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p RIGHT JOIN t_ir_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'inner all fixedstring', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.kfs = b.kfs SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'full all nullable', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p FULL JOIN t_ir_build AS b ON p.knull = b.knull SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'inner all lowcardinality build vs plain probe', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.klc = b.klc SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'inner all two keys (keys128)', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.k = b.k AND p.k2 = b.k2 SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'right all three keys (keys256)', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p RIGHT JOIN t_ir_build AS b ON p.k = b.k AND p.k2 = b.k2 AND p.k3 = b.k3 SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'inner all string plus uint64 (hashed)', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.ks = b.ks AND p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'inner all using', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b USING (k) SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'left all on filter right side', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p LEFT JOIN t_ir_build AS b ON p.k = b.k AND b.v % 3 = 0 SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'right all on filter right side (filtered rows are non-joined)', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p RIGHT JOIN t_ir_build AS b ON p.k = b.k AND b.v % 3 = 0 SETTINGS join_algorithm = 'hash') AS pa);

-- `UInt8` and `UInt16` keys use a direct-index table, which is never partitioned and stores duplicates
-- without runs.
SELECT 'inner all uint16 (fixed map)', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT * FROM t_ir_probe WHERE k < 20000) AS p INNER JOIN t_ir_build AS b ON p.k16 = b.k16 SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'right all uint8 (fixed map)', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT * FROM t_ir_probe WHERE k < 300) AS p RIGHT JOIN (SELECT * FROM t_ir_build WHERE k < 3000) AS b ON p.k8 = b.k8 SETTINGS join_algorithm = 'hash') AS pa);

-- ASOF picks among equal `ts` values in insertion order. That order depends on the block arrival
-- order. The narrow-key builds are therefore restricted to ranges where the narrow key is unique
-- per `k` (no ties).
SELECT 'asof inner >= uint16 (fixed map)', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT * FROM t_ir_probe WHERE k < 20000) AS p ASOF JOIN (SELECT * FROM t_ir_build WHERE k < 50000) AS b ON p.k16 = b.k16 AND p.ts >= b.ts SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'asof left < uint8 (fixed map)', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT * FROM t_ir_probe WHERE k < 300) AS p ASOF LEFT JOIN (SELECT * FROM t_ir_build WHERE k < 200) AS b ON p.k8 = b.k8 AND p.ts < b.ts SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'asof inner >=', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p ASOF JOIN t_ir_build AS b ON p.k = b.k AND p.ts >= b.ts SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'asof left <', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p ASOF LEFT JOIN t_ir_build AS b ON p.k = b.k AND p.ts < b.ts SETTINGS join_algorithm = 'hash') AS pa);

-- Two narrow keys pack into one word (`keys32`, `keys64`).
SELECT 'inner all packed keys32 (uint16, uint8)', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT * FROM t_ir_probe WHERE k < 60000) AS p INNER JOIN t_ir_build AS b ON p.k16 = b.k16 AND p.k8 = b.k8 SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'right all packed keys64 (uint32, uint16)', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p RIGHT JOIN t_ir_build AS b ON p.k32 = b.k32 AND p.k16 = b.k16 SETTINGS join_algorithm = 'hash') AS pa);

-- The zero key (kept in the table's separate zero cell) with many duplicates, probed many times, and as a
-- non-joined key; the same for the empty string.
SELECT 'inner all zero key with duplicates', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT if(k % 991 = 0, 0, k) AS k, p FROM t_ir_probe) AS p INNER JOIN (SELECT if(k % 977 = 0, 0, k) AS k, v FROM t_ir_build) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'right anti zero key with duplicates', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v))) FROM (SELECT k, p FROM t_ir_probe WHERE k % 2 = 1) AS p ANTI RIGHT JOIN (SELECT if(k % 977 = 0, 0, k) AS k, v FROM t_ir_build) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'left all empty-string key with duplicates', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT if(k % 991 = 0, '', ks) AS ks, p FROM t_ir_probe) AS p LEFT JOIN (SELECT if(k % 977 = 0, '', ks) AS ks, v FROM t_ir_build) AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash') AS pa);

-- Probe-side filter in the ON clause; the legacy distinct-right-keys ANY and the take-last-row ANY
-- (key-determined expressions, the chosen row is arbitrary).
SELECT 'left all on filter left side', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p LEFT JOIN t_ir_build AS b ON p.k = b.k AND p.p % 3 = 0 SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'right any distinct right keys', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_ir_probe AS p ANY RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', any_join_distinct_right_table_keys = 1) AS pa);

SELECT 'left any take last row', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_ir_probe AS p ANY LEFT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', join_any_take_last_row = 1) AS pa);

-- The other ASOF directions and a String equi-key (`ts` is distinct within a key: no ties).
SELECT 'asof inner >', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p ASOF JOIN t_ir_build AS b ON p.k = b.k AND p.ts > b.ts SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'asof left <=', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p ASOF LEFT JOIN t_ir_build AS b ON p.k = b.k AND p.ts <= b.ts SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'asof inner >= string key', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p ASOF JOIN t_ir_build AS b ON p.ks = b.ks AND p.ts >= b.ts SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'inner all uint64 max_threads 1', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', max_threads = 1) AS pa);

SELECT 'right all uint64 max_threads 16', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', max_threads = 16) AS pa);

SELECT 'empty build', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p LEFT JOIN (SELECT * FROM t_ir_build WHERE 0) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'empty probe', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT * FROM t_ir_probe WHERE 0) AS p RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS pa);

SELECT 'all-miss probe', pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p LEFT JOIN t_ir_build AS b ON p.k + 10000000 = b.k SETTINGS join_algorithm = 'hash') AS pa);

SYSTEM FLUSH LOGS query_log;

-- The partitioned build ran: more than one partition, a non-empty hash table, every build row inserted, and
-- duplicate storage used.
SELECT
    ProfileEvents['HashJoinPartitions'] > 1,
    ProfileEvents['HashJoinTableBytes'] > 0,
    ProfileEvents['HashJoinInsertedRows'] = (SELECT count() FROM t_ir_build),
    ProfileEvents['HashJoinDuplicateRunBytes'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05111 inner all uint64' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_ir_build;
DROP TABLE t_ir_probe;
