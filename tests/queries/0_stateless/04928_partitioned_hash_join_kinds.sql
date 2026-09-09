-- Join-kind matrix of `join_algorithm = 'partitioned_hash'` against `hash` AND `parallel_hash`:
-- INNER/LEFT/RIGHT/FULL x ALL/ANY/SEMI/ANTI over a build side large enough to partition, with
-- RIGHT/FULL non-joined rows, dedup (ANY/SEMI/ANTI) semantics, `join_use_nulls`, NULLs in keys,
-- a duplicate-heavy right side, USING, and empty sides. Engagement of the partitioned path is
-- asserted through ProfileEvents at the end. ANY/SEMI joins pick an arbitrary row per key, so
-- their checksums use only key-determined expressions.

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

SELECT 'right all uint64';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4kind right all uint64';

SELECT 'right all uint64 sample';
SELECT p.pv, b.v FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.k64 = b.k64 ORDER BY b.v, p.pv LIMIT 3 SETTINGS join_algorithm = 'hash';
SELECT p.pv, b.v FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.k64 = b.k64 ORDER BY b.v, p.pv LIMIT 3 SETTINGS join_algorithm = 'parallel_hash';
SELECT p.pv, b.v FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.k64 = b.k64 ORDER BY b.v, p.pv LIMIT 3 SETTINGS join_algorithm = 'partitioned_hash';

SELECT 'right all string join_use_nulls';
SELECT count(), sum(cityHash64(ifNull(p.pv, 0), b.v)) FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash', join_use_nulls = 1;
SELECT count(), sum(cityHash64(ifNull(p.pv, 0), b.v)) FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.ks = b.ks SETTINGS join_algorithm = 'parallel_hash', join_use_nulls = 1;
SELECT count(), sum(cityHash64(ifNull(p.pv, 0), b.v)) FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash', join_use_nulls = 1, log_comment = 'p4kind right all string jun';

SELECT 'right all nullable keys';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.knull = b.knull SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.knull = b.knull SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_kp AS p RIGHT JOIN t_kb AS b ON p.knull = b.knull SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4kind right all nullable';

SELECT 'full all uint64';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_kp AS p FULL JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_kp AS p FULL JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_kp AS p FULL JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4kind full all uint64';

SELECT 'full all string join_use_nulls';
SELECT count(), sum(cityHash64(ifNull(p.pv, 0), ifNull(b.v, 0))) FROM t_kp AS p FULL JOIN t_kb AS b ON p.ks = b.ks SETTINGS join_algorithm = 'hash', join_use_nulls = 1;
SELECT count(), sum(cityHash64(ifNull(p.pv, 0), ifNull(b.v, 0))) FROM t_kp AS p FULL JOIN t_kb AS b ON p.ks = b.ks SETTINGS join_algorithm = 'parallel_hash', join_use_nulls = 1;
SELECT count(), sum(cityHash64(ifNull(p.pv, 0), ifNull(b.v, 0))) FROM t_kp AS p FULL JOIN t_kb AS b ON p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash', join_use_nulls = 1, log_comment = 'p4kind full all string jun';

SELECT 'right all duplicate-heavy build (single leaf)';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_kp AS p RIGHT JOIN (SELECT kdup, v FROM t_kb ORDER BY v LIMIT 30000) AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_kp AS p RIGHT JOIN (SELECT kdup, v FROM t_kb ORDER BY v LIMIT 30000) AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_kp AS p RIGHT JOIN (SELECT kdup, v FROM t_kb ORDER BY v LIMIT 30000) AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4dup right all dup-heavy';

SELECT 'any left (key-determined checksum)';
SELECT count(), sum(cityHash64(p.pv, b.k64 * 10)) FROM t_kp AS p ANY LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.pv, b.k64 * 10)) FROM t_kp AS p ANY LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(p.pv, b.k64 * 10)) FROM t_kp AS p ANY LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4kind any left';

SELECT 'any inner dup-heavy (key-determined checksum)';
SELECT count(), sum(cityHash64(p.kdup)) FROM t_kp AS p ANY INNER JOIN t_kb AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.kdup)) FROM t_kp AS p ANY INNER JOIN t_kb AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(p.kdup)) FROM t_kp AS p ANY INNER JOIN t_kb AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4dup any inner';

SELECT 'any right';
SELECT count(), sum(cityHash64(b.v)) FROM t_kp AS p ANY RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(b.v)) FROM t_kp AS p ANY RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(b.v)) FROM t_kp AS p ANY RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4kind any right';

SELECT 'any left take_last_row (key-determined checksum)';
SELECT count(), sum(cityHash64(p.kdup, b.kdup * 7)) FROM t_kp AS p ANY LEFT JOIN t_kb AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'hash', join_any_take_last_row = 1;
SELECT count(), sum(cityHash64(p.kdup, b.kdup * 7)) FROM t_kp AS p ANY LEFT JOIN t_kb AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'parallel_hash', join_any_take_last_row = 1;
SELECT count(), sum(cityHash64(p.kdup, b.kdup * 7)) FROM t_kp AS p ANY LEFT JOIN t_kb AS b ON p.kdup = b.kdup SETTINGS join_algorithm = 'partitioned_hash', join_any_take_last_row = 1, log_comment = 'p4dup any left last row';

SELECT 'legacy rightany (any_join_distinct_right_table_keys)';
SELECT count(), sum(cityHash64(p.pv, b.k64)) FROM t_kp AS p ANY LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash', any_join_distinct_right_table_keys = 1;
SELECT count(), sum(cityHash64(p.pv, b.k64)) FROM t_kp AS p ANY LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash', any_join_distinct_right_table_keys = 1;
SELECT count(), sum(cityHash64(p.pv, b.k64)) FROM t_kp AS p ANY LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash', any_join_distinct_right_table_keys = 1, log_comment = 'p4kind legacy rightany';

SELECT 'semi left';
SELECT count(), sum(cityHash64(p.pv)) FROM t_kp AS p SEMI LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.pv)) FROM t_kp AS p SEMI LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(p.pv)) FROM t_kp AS p SEMI LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4kind semi left';

SELECT 'semi right';
SELECT count(), sum(cityHash64(b.v)) FROM t_kp AS p SEMI RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(b.v)) FROM t_kp AS p SEMI RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(b.v)) FROM t_kp AS p SEMI RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4kind semi right';

SELECT 'anti left';
SELECT count(), sum(cityHash64(p.pv)) FROM t_kp AS p ANTI LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.pv)) FROM t_kp AS p ANTI LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(p.pv)) FROM t_kp AS p ANTI LEFT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4kind anti left';

SELECT 'anti right';
SELECT count(), sum(cityHash64(b.v)) FROM t_kp AS p ANTI RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(b.v)) FROM t_kp AS p ANTI RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(b.v)) FROM t_kp AS p ANTI RIGHT JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4kind anti right';

SELECT 'right using';
SELECT count(), sum(cityHash64(k64, b.v)) FROM t_kp AS p RIGHT JOIN t_kb AS b USING (k64) SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(k64, b.v)) FROM t_kp AS p RIGHT JOIN t_kb AS b USING (k64) SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(k64, b.v)) FROM t_kp AS p RIGHT JOIN t_kb AS b USING (k64) SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4kind right using';

SELECT 'empty build side right';
SELECT count() FROM t_kp AS p RIGHT JOIN (SELECT * FROM t_kb WHERE 0) AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash';
SELECT count() FROM t_kp AS p RIGHT JOIN (SELECT * FROM t_kb WHERE 0) AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash';

SELECT 'empty probe side full';
SELECT count(), sum(cityHash64(b.v)) FROM (SELECT * FROM t_kp WHERE 0) AS p FULL JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(b.v)) FROM (SELECT * FROM t_kp WHERE 0) AS p FULL JOIN t_kb AS b ON p.k64 = b.k64 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4kind empty probe full';

-- Engagement assertions: the big builds must partition, and the leaf builds and probes must run
-- through the partitioned path (nonzero leaf rows, no heap fallbacks); the duplicate-heavy and
-- small-key builds may degenerate to a single leaf but still engage the algorithm.
SYSTEM FLUSH LOGS query_log;
SELECT 'partition plans';
SELECT
    log_comment,
    ProfileEvents['PartitionedHashJoinPartitions'] > 1,
    ProfileEvents['PartitionedHashJoinLeafRows'] > 0,
    ProfileEvents['PartitionedHashJoinHashTableGrowths']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'p4kind %'
ORDER BY log_comment;
SELECT
    log_comment,
    ProfileEvents['PartitionedHashJoinPartitions'] >= 1,
    ProfileEvents['PartitionedHashJoinLeafRows'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'p4dup %'
ORDER BY log_comment;

DROP TABLE t_kb;
DROP TABLE t_kp;
