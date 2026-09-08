-- Result parity of `join_algorithm = 'partitioned_hash'` with `hash` over a duplicate-heavy build whose
-- keys carry 1, 2, 3, 7, 8, 9 and 300 rows each, spread over blocks, so every duplicate layout of the
-- shared hash table - inline, pair, run, run list - is produced and read back: across join kinds and
-- strictnesses, key types, ON filters, USING, `join_use_nulls`, thread counts, and empty or all-miss
-- inputs. Each line prints the two results and whether they are equal.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
-- Automatic external join would route to SpillingHashJoin at plan time (the absolute setting can also
-- arrive through test-level randomization, so pin both).
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

SELECT 'partitioned_hash is selected', count() > 0 FROM (EXPLAIN actions = 1 SELECT p.p FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') WHERE explain LIKE '%Algorithm: PartitionedHashJoin%';

SELECT 'inner all uint64', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q)
SETTINGS log_comment = '05111 inner all uint64';

SELECT 'left all uint64', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p LEFT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p LEFT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'right all uint64', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'full all uint64', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p FULL JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p FULL JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'full all uint64 join_use_nulls', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(ifNull(p.p, 0), ifNull(b.v, 0)))) FROM t_ir_probe AS p FULL JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', join_use_nulls = 1) AS h,
    (SELECT (count(), sum(cityHash64(ifNull(p.p, 0), ifNull(b.v, 0)))) FROM t_ir_probe AS p FULL JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', join_use_nulls = 1) AS q);

-- ANY picks an arbitrary row per key, so only key-determined expressions are compared.
SELECT 'inner any uint64', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_ir_probe AS p ANY INNER JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_ir_probe AS p ANY INNER JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'left any uint64', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_ir_probe AS p ANY LEFT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_ir_probe AS p ANY LEFT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'right any uint64', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v))) FROM t_ir_probe AS p ANY RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v))) FROM t_ir_probe AS p ANY RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'left semi uint64', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_ir_probe AS p SEMI LEFT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_ir_probe AS p SEMI LEFT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'left anti uint64', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p))) FROM t_ir_probe AS p ANTI LEFT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p))) FROM t_ir_probe AS p ANTI LEFT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'right semi uint64', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v))) FROM t_ir_probe AS p SEMI RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v))) FROM t_ir_probe AS p SEMI RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'right anti uint64', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v))) FROM (SELECT * FROM t_ir_probe WHERE k % 2 = 0) AS p ANTI RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v))) FROM (SELECT * FROM t_ir_probe WHERE k % 2 = 0) AS p ANTI RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'inner all string', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'right all string', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p RIGHT JOIN t_ir_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p RIGHT JOIN t_ir_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'inner all fixedstring', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.kfs = b.kfs SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.kfs = b.kfs SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'full all nullable', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p FULL JOIN t_ir_build AS b ON p.knull = b.knull SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p FULL JOIN t_ir_build AS b ON p.knull = b.knull SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'inner all lowcardinality build vs plain probe', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.klc = b.klc SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.klc = b.klc SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'inner all two keys (keys128)', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.k = b.k AND p.k2 = b.k2 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.k = b.k AND p.k2 = b.k2 SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'right all three keys (keys256)', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p RIGHT JOIN t_ir_build AS b ON p.k = b.k AND p.k2 = b.k2 AND p.k3 = b.k3 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p RIGHT JOIN t_ir_build AS b ON p.k = b.k AND p.k2 = b.k2 AND p.k3 = b.k3 SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'inner all string plus uint64 (hashed)', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.ks = b.ks AND p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.ks = b.ks AND p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'inner all using', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b USING (k) SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b USING (k) SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'left all on filter right side', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p LEFT JOIN t_ir_build AS b ON p.k = b.k AND b.v % 3 = 0 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p LEFT JOIN t_ir_build AS b ON p.k = b.k AND b.v % 3 = 0 SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'right all on filter right side (filtered rows are non-joined)', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p RIGHT JOIN t_ir_build AS b ON p.k = b.k AND b.v % 3 = 0 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p RIGHT JOIN t_ir_build AS b ON p.k = b.k AND b.v % 3 = 0 SETTINGS join_algorithm = 'partitioned_hash') AS q);

-- The direct-index map types (`key8`, `key16`) have no ranges; they take the fixed-map insert path.
SELECT 'inner all uint16 (fixed map)', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT * FROM t_ir_probe WHERE k < 20000) AS p INNER JOIN t_ir_build AS b ON p.k16 = b.k16 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT * FROM t_ir_probe WHERE k < 20000) AS p INNER JOIN t_ir_build AS b ON p.k16 = b.k16 SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'right all uint8 (fixed map)', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT * FROM t_ir_probe WHERE k < 300) AS p RIGHT JOIN (SELECT * FROM t_ir_build WHERE k < 3000) AS b ON p.k8 = b.k8 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT * FROM t_ir_probe WHERE k < 300) AS p RIGHT JOIN (SELECT * FROM t_ir_build WHERE k < 3000) AS b ON p.k8 = b.k8 SETTINGS join_algorithm = 'partitioned_hash') AS q);

-- ASOF picks among equal `ts` values in insertion order, which depends on the block arrival order, so the
-- narrow-key builds are restricted to ranges where the narrow key is unique per `k` (no ties).
SELECT 'asof inner >= uint16 (fixed map)', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT * FROM t_ir_probe WHERE k < 20000) AS p ASOF JOIN (SELECT * FROM t_ir_build WHERE k < 50000) AS b ON p.k16 = b.k16 AND p.ts >= b.ts SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT * FROM t_ir_probe WHERE k < 20000) AS p ASOF JOIN (SELECT * FROM t_ir_build WHERE k < 50000) AS b ON p.k16 = b.k16 AND p.ts >= b.ts SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'asof left < uint8 (fixed map)', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT * FROM t_ir_probe WHERE k < 300) AS p ASOF LEFT JOIN (SELECT * FROM t_ir_build WHERE k < 200) AS b ON p.k8 = b.k8 AND p.ts < b.ts SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT * FROM t_ir_probe WHERE k < 300) AS p ASOF LEFT JOIN (SELECT * FROM t_ir_build WHERE k < 200) AS b ON p.k8 = b.k8 AND p.ts < b.ts SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'asof inner >=', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p ASOF JOIN t_ir_build AS b ON p.k = b.k AND p.ts >= b.ts SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p ASOF JOIN t_ir_build AS b ON p.k = b.k AND p.ts >= b.ts SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'asof left <', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p ASOF LEFT JOIN t_ir_build AS b ON p.k = b.k AND p.ts < b.ts SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p ASOF LEFT JOIN t_ir_build AS b ON p.k = b.k AND p.ts < b.ts SETTINGS join_algorithm = 'partitioned_hash') AS q);

-- Two narrow keys pack into one word (`keys32`, `keys64`).
SELECT 'inner all packed keys32 (uint16, uint8)', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT * FROM t_ir_probe WHERE k < 60000) AS p INNER JOIN t_ir_build AS b ON p.k16 = b.k16 AND p.k8 = b.k8 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT * FROM t_ir_probe WHERE k < 60000) AS p INNER JOIN t_ir_build AS b ON p.k16 = b.k16 AND p.k8 = b.k8 SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'right all packed keys64 (uint32, uint16)', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p RIGHT JOIN t_ir_build AS b ON p.k32 = b.k32 AND p.k16 = b.k16 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p RIGHT JOIN t_ir_build AS b ON p.k32 = b.k32 AND p.k16 = b.k16 SETTINGS join_algorithm = 'partitioned_hash') AS q);

-- The zero key (the table's out-of-buffer cell) with many duplicates, probed many times, and as a
-- non-joined key; the same for the empty string.
SELECT 'inner all zero key with duplicates', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT if(k % 991 = 0, 0, k) AS k, p FROM t_ir_probe) AS p INNER JOIN (SELECT if(k % 977 = 0, 0, k) AS k, v FROM t_ir_build) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT if(k % 991 = 0, 0, k) AS k, p FROM t_ir_probe) AS p INNER JOIN (SELECT if(k % 977 = 0, 0, k) AS k, v FROM t_ir_build) AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'right anti zero key with duplicates', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v))) FROM (SELECT k, p FROM t_ir_probe WHERE k % 2 = 1) AS p ANTI RIGHT JOIN (SELECT if(k % 977 = 0, 0, k) AS k, v FROM t_ir_build) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v))) FROM (SELECT k, p FROM t_ir_probe WHERE k % 2 = 1) AS p ANTI RIGHT JOIN (SELECT if(k % 977 = 0, 0, k) AS k, v FROM t_ir_build) AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'left all empty-string key with duplicates', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT if(k % 991 = 0, '', ks) AS ks, p FROM t_ir_probe) AS p LEFT JOIN (SELECT if(k % 977 = 0, '', ks) AS ks, v FROM t_ir_build) AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT if(k % 991 = 0, '', ks) AS ks, p FROM t_ir_probe) AS p LEFT JOIN (SELECT if(k % 977 = 0, '', ks) AS ks, v FROM t_ir_build) AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash') AS q);

-- Probe-side filter in the ON clause; the legacy distinct-right-keys ANY and the take-last-row ANY
-- (key-determined expressions, the chosen row is arbitrary).
SELECT 'left all on filter left side', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p LEFT JOIN t_ir_build AS b ON p.k = b.k AND p.p % 3 = 0 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p LEFT JOIN t_ir_build AS b ON p.k = b.k AND p.p % 3 = 0 SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'right any distinct right keys', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_ir_probe AS p ANY RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', any_join_distinct_right_table_keys = 1) AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_ir_probe AS p ANY RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', any_join_distinct_right_table_keys = 1) AS q);

SELECT 'left any take last row', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_ir_probe AS p ANY LEFT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash', join_any_take_last_row = 1) AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.k))) FROM t_ir_probe AS p ANY LEFT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', join_any_take_last_row = 1) AS q);

-- The other ASOF directions and a String equi-key (`ts` is distinct within a key: no ties).
SELECT 'asof inner >', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p ASOF JOIN t_ir_build AS b ON p.k = b.k AND p.ts > b.ts SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p ASOF JOIN t_ir_build AS b ON p.k = b.k AND p.ts > b.ts SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'asof left <=', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p ASOF LEFT JOIN t_ir_build AS b ON p.k = b.k AND p.ts <= b.ts SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p ASOF LEFT JOIN t_ir_build AS b ON p.k = b.k AND p.ts <= b.ts SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'asof inner >= string key', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p ASOF JOIN t_ir_build AS b ON p.ks = b.ks AND p.ts >= b.ts SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p ASOF JOIN t_ir_build AS b ON p.ks = b.ks AND p.ts >= b.ts SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'inner all uint64 max_threads 1', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p INNER JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_threads = 1) AS q);

SELECT 'right all uint64 max_threads 16', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash', max_threads = 16) AS q);

SELECT 'empty build', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p LEFT JOIN (SELECT * FROM t_ir_build WHERE 0) AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p LEFT JOIN (SELECT * FROM t_ir_build WHERE 0) AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'empty probe', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT * FROM t_ir_probe WHERE 0) AS p RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM (SELECT * FROM t_ir_probe WHERE 0) AS p RIGHT JOIN t_ir_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

SELECT 'all-miss probe', h = q, h, q FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p LEFT JOIN t_ir_build AS b ON p.k + 10000000 = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_ir_probe AS p LEFT JOIN t_ir_build AS b ON p.k + 10000000 = b.k SETTINGS join_algorithm = 'partitioned_hash') AS q);

SYSTEM FLUSH LOGS query_log;

-- The partitioned path did engage: more than one owner range, one table whose bytes are accounted, every
-- build row inserted, and duplicate runs written.
SELECT
    ProfileEvents['PartitionedHashJoinPartitions'] > 1,
    ProfileEvents['PartitionedHashJoinHashTableBytes'] > 0,
    ProfileEvents['PartitionedHashJoinLeafRows'] = (SELECT count() FROM t_ir_build),
    ProfileEvents['PartitionedHashJoinDuplicateRunBytes'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05111 inner all uint64' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_ir_build;
DROP TABLE t_ir_probe;
