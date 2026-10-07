-- Join-kind matrix of `join_algorithm = 'partitioned_hash'` against `hash` and `parallel_hash`.
-- INNER/LEFT/RIGHT/FULL crossed with ALL/ANY/SEMI/ANTI, over a build side large enough to partition.
-- Coverage includes RIGHT/FULL non-joined rows, ANY/SEMI/ANTI dedup, `join_use_nulls`, NULLs in
-- keys, a duplicate-heavy right side, USING, and empty sides. The query log check at the end
-- asserts that the partitioned build ran. ANY/SEMI joins pick an arbitrary row per key, so their
-- checksums use only key-determined expressions.

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

DROP TABLE IF EXISTS t_kb;
DROP TABLE IF EXISTS t_kp;

CREATE TABLE t_kb ENGINE = MergeTree ORDER BY tuple() AS
SELECT
    number * 2 AS k64,                                     -- even keys 0..599998: half match the probe
    toString(number * 2) AS ks,
    if(number % 9 = 0, NULL, toUInt64(number * 2)) AS knull,
    number % 1000 AS kdup,                                 -- 1000 distinct keys x 300 duplicates
    number + 1000000000 AS v
FROM numbers(300000);

CREATE TABLE t_kp ENGINE = MergeTree ORDER BY tuple() AS
SELECT
    number AS k64,                                         -- 0..399999: even ones match the build
    toString(number) AS ks,
    if(number % 7 = 0, NULL, toUInt64(number)) AS knull,
    number % 1500 AS kdup,
    number + 2000000000 AS pv
FROM numbers(400000);

SELECT 'right all uint64', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04928 right all uint64';

SELECT 'right all uint64 sample';
SELECT p.pv, b.v FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.k64 = b.k64 ORDER BY b.v, p.pv LIMIT 3 SETTINGS join_algorithm = 'hash';
SELECT p.pv, b.v FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.k64 = b.k64 ORDER BY b.v, p.pv LIMIT 3 SETTINGS join_algorithm = 'parallel_hash';
SELECT p.pv, b.v FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.k64 = b.k64 ORDER BY b.v, p.pv LIMIT 3 SETTINGS join_algorithm = 'partitioned_hash';

SELECT 'right all string join_use_nulls', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(ifNull(p.pv, 0), b.v))) FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash', join_use_nulls = 1) AS h,
    (SELECT (count(), sum(cityHash64(ifNull(p.pv, 0), b.v))) FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.ks = b.ks SETTINGS join_algorithm = 'parallel_hash', join_use_nulls = 1) AS ph,
    (SELECT (count(), sum(cityHash64(ifNull(p.pv, 0), b.v))) FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash', join_use_nulls = 1) AS pa)
SETTINGS log_comment = '04928 right all string join_use_nulls';

SELECT 'right all nullable keys', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.knull = b.knull SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.knull = b.knull SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.knull = b.knull SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04928 right all nullable';

SELECT 'full all uint64', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_kp AS p FULL JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_kp AS p FULL JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_kp AS p FULL JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04928 full all uint64';

SELECT 'full all string join_use_nulls', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(ifNull(p.pv, 0), ifNull(b.v, 0)))) FROM t_kp AS p FULL JOIN t_kb AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash', join_use_nulls = 1) AS h,
    (SELECT (count(), sum(cityHash64(ifNull(p.pv, 0), ifNull(b.v, 0)))) FROM t_kp AS p FULL JOIN t_kb AS b ON p.ks = b.ks SETTINGS join_algorithm = 'parallel_hash', join_use_nulls = 1) AS ph,
    (SELECT (count(), sum(cityHash64(ifNull(p.pv, 0), ifNull(b.v, 0)))) FROM t_kp AS p FULL JOIN t_kb AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash', join_use_nulls = 1) AS pa)
SETTINGS log_comment = '04928 full all string join_use_nulls';

SELECT 'right all duplicate-heavy build (1000 keys, may be one partition)', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_kp AS p RIGHT JOIN (SELECT kdup, v FROM t_kb ORDER BY v LIMIT 30000) AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_kp AS p RIGHT JOIN (SELECT kdup, v FROM t_kb ORDER BY v LIMIT 30000) AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_kp AS p RIGHT JOIN (SELECT kdup, v FROM t_kb ORDER BY v LIMIT 30000) AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04928 dup right all';

SELECT 'any left (key-determined checksum)', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.pv, b.k64 * 10))) FROM t_kp AS p ANY LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.pv, b.k64 * 10))) FROM t_kp AS p ANY LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.pv, b.k64 * 10))) FROM t_kp AS p ANY LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04928 any left';

SELECT 'any inner dup-heavy (key-determined checksum)', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.kdup))) FROM t_kp AS p ANY INNER JOIN t_kb AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.kdup))) FROM t_kp AS p ANY INNER JOIN t_kb AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.kdup))) FROM t_kp AS p ANY INNER JOIN t_kb AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04928 dup any inner';

SELECT 'any right', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v))) FROM t_kp AS p ANY RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v))) FROM t_kp AS p ANY RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(b.v))) FROM t_kp AS p ANY RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04928 any right';

SELECT 'any left take_last_row (key-determined checksum)', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.kdup, b.kdup * 7))) FROM t_kp AS p ANY LEFT JOIN t_kb AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'hash', join_any_take_last_row = 1) AS h,
    (SELECT (count(), sum(cityHash64(p.kdup, b.kdup * 7))) FROM t_kp AS p ANY LEFT JOIN t_kb AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'parallel_hash', join_any_take_last_row = 1) AS ph,
    (SELECT (count(), sum(cityHash64(p.kdup, b.kdup * 7))) FROM t_kp AS p ANY LEFT JOIN t_kb AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'partitioned_hash', join_any_take_last_row = 1) AS pa)
SETTINGS log_comment = '04928 dup any left last row';

SELECT 'legacy rightany (any_join_distinct_right_table_keys)', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.pv, b.k64))) FROM t_kp AS p ANY LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash', any_join_distinct_right_table_keys = 1) AS h,
    (SELECT (count(), sum(cityHash64(p.pv, b.k64))) FROM t_kp AS p ANY LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash', any_join_distinct_right_table_keys = 1) AS ph,
    (SELECT (count(), sum(cityHash64(p.pv, b.k64))) FROM t_kp AS p ANY LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash', any_join_distinct_right_table_keys = 1) AS pa)
SETTINGS log_comment = '04928 legacy rightany';

SELECT 'semi left', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.pv))) FROM t_kp AS p SEMI LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.pv))) FROM t_kp AS p SEMI LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.pv))) FROM t_kp AS p SEMI LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04928 semi left';

SELECT 'semi right', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v))) FROM t_kp AS p SEMI RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v))) FROM t_kp AS p SEMI RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(b.v))) FROM t_kp AS p SEMI RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04928 semi right';

SELECT 'anti left', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.pv))) FROM t_kp AS p ANTI LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.pv))) FROM t_kp AS p ANTI LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.pv))) FROM t_kp AS p ANTI LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04928 anti left';

SELECT 'anti right', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v))) FROM t_kp AS p ANTI RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v))) FROM t_kp AS p ANTI RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(b.v))) FROM t_kp AS p ANTI RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04928 anti right';

SELECT 'right using', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(k64, b.v))) FROM t_kp AS p RIGHT JOIN t_kb AS b USING (k64) SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(k64, b.v))) FROM t_kp AS p RIGHT JOIN t_kb AS b USING (k64) SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(k64, b.v))) FROM t_kp AS p RIGHT JOIN t_kb AS b USING (k64) SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04928 right using';

SELECT 'empty build side right', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(p.pv)) FROM t_kp AS p RIGHT JOIN (SELECT * FROM t_kb WHERE 0) AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(p.pv)) FROM t_kp AS p RIGHT JOIN (SELECT * FROM t_kb WHERE 0) AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash') AS pa);

SELECT 'empty probe side full', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(b.v))) FROM (SELECT * FROM t_kp WHERE 0) AS p FULL JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(b.v))) FROM (SELECT * FROM t_kp WHERE 0) AS p FULL JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04928 empty probe full';

-- The large builds must use more than one partition and insert rows (`HashJoinInsertedRows` > 0).
-- The duplicate-heavy build over 1000 keys may end with one partition but must still insert its rows.
SYSTEM FLUSH LOGS query_log;

SELECT 'partition plans';
SELECT
    log_comment,
    ProfileEvents['HashJoinPartitions'] > 1,
    ProfileEvents['HashJoinInsertedRows'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '04928 %' AND log_comment NOT LIKE '04928 dup %'
ORDER BY log_comment;
SELECT
    log_comment,
    ProfileEvents['HashJoinPartitions'] >= 1,
    ProfileEvents['HashJoinInsertedRows'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '04928 dup %'
ORDER BY log_comment;

DROP TABLE t_kb;
DROP TABLE t_kp;
