-- Result parity of `join_algorithm = 'partitioned_hash'` with `hash` and `parallel_hash`,
-- plus plan-time fallback of shapes the partitioned algorithm does not support.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;

DROP TABLE IF EXISTS t_phj_build;
DROP TABLE IF EXISTS t_phj_probe;

CREATE TABLE t_phj_build
(
    k UInt64,
    ks String,
    kn Nullable(UInt32),
    klc LowCardinality(String),
    v String
)
ENGINE = MergeTree ORDER BY k;

CREATE TABLE t_phj_probe
(
    k UInt64,
    ks String,
    kn Nullable(UInt32),
    klc LowCardinality(String),
    p UInt64
)
ENGINE = MergeTree ORDER BY k;

INSERT INTO t_phj_build
SELECT
    number % 100,
    concat('s', toString(number % 77)),
    if(number % 10 = 0, NULL, toUInt32(number % 50)),
    concat('lc', toString(number % 13)),
    concat('v', toString(number))
FROM numbers(1000);

INSERT INTO t_phj_probe
SELECT
    number % 150,
    concat('s', toString(number % 90)),
    if(number % 7 = 0, NULL, toUInt32(number % 60)),
    concat('lc', toString(number % 17)),
    number
FROM numbers(2000);

SELECT '-- partitioned_hash is selected for INNER ALL';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT p.p FROM t_phj_probe AS p INNER JOIN t_phj_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') WHERE explain LIKE '%Algorithm: PartitionedHashJoin%';

SELECT 'INNER ALL, UInt64 key', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p INNER JOIN t_phj_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p INNER JOIN t_phj_build AS b ON p.k = b.k SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p INNER JOIN t_phj_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT '-- INNER ALL, UInt64 key, first rows';
SELECT p.p, b.v FROM t_phj_probe AS p INNER JOIN t_phj_build AS b ON p.k = b.k ORDER BY p.p, b.v LIMIT 5 SETTINGS join_algorithm = 'hash';
SELECT p.p, b.v FROM t_phj_probe AS p INNER JOIN t_phj_build AS b ON p.k = b.k ORDER BY p.p, b.v LIMIT 5 SETTINGS join_algorithm = 'parallel_hash';
SELECT p.p, b.v FROM t_phj_probe AS p INNER JOIN t_phj_build AS b ON p.k = b.k ORDER BY p.p, b.v LIMIT 5 SETTINGS join_algorithm = 'partitioned_hash';

SELECT 'INNER ALL, String key', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p INNER JOIN t_phj_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p INNER JOIN t_phj_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p INNER JOIN t_phj_build AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'INNER ALL, Nullable key (NULLs never join)', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p INNER JOIN t_phj_build AS b ON p.kn = b.kn SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p INNER JOIN t_phj_build AS b ON p.kn = b.kn SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p INNER JOIN t_phj_build AS b ON p.kn = b.kn SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'INNER ALL, LowCardinality(String) key', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p INNER JOIN t_phj_build AS b ON p.klc = b.klc SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p INNER JOIN t_phj_build AS b ON p.klc = b.klc SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p INNER JOIN t_phj_build AS b ON p.klc = b.klc SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'INNER ALL, composite key (UInt64, String)', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p INNER JOIN t_phj_build AS b ON p.k = b.k AND p.ks = b.ks SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p INNER JOIN t_phj_build AS b ON p.k = b.k AND p.ks = b.ks SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p INNER JOIN t_phj_build AS b ON p.k = b.k AND p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'LEFT ALL, UInt64 key', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p LEFT JOIN t_phj_build AS b ON p.k = b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p LEFT JOIN t_phj_build AS b ON p.k = b.k SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p LEFT JOIN t_phj_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT '-- a mixed non-equi ON condition runs under partitioned_hash (05218 covers every shape)';
SELECT count() FROM (EXPLAIN actions = 1 SELECT p.p FROM t_phj_probe AS p LEFT JOIN t_phj_build AS b ON p.k = b.k AND p.p > b.k SETTINGS join_algorithm = 'partitioned_hash') WHERE explain LIKE '%Algorithm: PartitionedHashJoin%';

SELECT 'mixed ON condition', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p LEFT JOIN t_phj_build AS b ON p.k = b.k AND p.p > b.k SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.p, b.v))) FROM t_phj_probe AS p LEFT JOIN t_phj_build AS b ON p.k = b.k AND p.p > b.k SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT '-- shapes beyond INNER/LEFT ALL execute under partitioned_hash (04928 and 04929 check their results)';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT p.p FROM t_phj_probe AS p RIGHT JOIN t_phj_build AS b ON p.k = b.k SETTINGS join_algorithm = 'partitioned_hash') WHERE explain LIKE '%Algorithm: PartitionedHashJoin%';

DROP TABLE t_phj_build;
DROP TABLE t_phj_probe;
