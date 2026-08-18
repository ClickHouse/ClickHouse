-- ASOF joins, per-clause ON-section filter conditions and multi-disjunct (OR of key sets) under
-- `join_algorithm = 'partitioned_hash'`, against `hash` AND `parallel_hash`. ASOF and
-- multi-disjunct plans stay at one partition by design (asserted at the end); the ON-filter
-- shapes partition normally, with the filtered-out right rows still reaching RIGHT/FULL
-- non-joined output. Mixed non-equi ON conditions fall back at plan time; 04926 asserts that.

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

DROP TABLE IF EXISTS t_ab;
DROP TABLE IF EXISTS t_ap;

CREATE TABLE t_ab ENGINE = MergeTree ORDER BY tuple() AS
SELECT
    number % 50000 AS k64,
    toString(number % 50000) AS ks,
    intDiv(number, 50000) * 100 AS ts,                         -- 6 time points per key: 0, 100, ..., 500
    if(number % 13 = 0, NULL, intDiv(number, 50000) * 100) AS tsnull,
    number AS kbig,                                            -- 300000 distinct keys: the ON-filter builds partition
    number + 1000000000 AS v
FROM numbers(300000);

CREATE TABLE t_ap ENGINE = MergeTree ORDER BY tuple() AS
SELECT
    number % 60000 AS k64,
    toString(number % 60000) AS ks,
    (number % 8) * 77 AS ts,
    number % 450000 AS kbig,
    number + 2000000000 AS pv
FROM numbers(400000);

SELECT 'asof inner >=';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p ASOF JOIN t_ab AS b ON p.k64 = b.k64 AND p.ts >= b.ts SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p ASOF JOIN t_ab AS b ON p.k64 = b.k64 AND p.ts >= b.ts SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p ASOF JOIN t_ab AS b ON p.k64 = b.k64 AND p.ts >= b.ts SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4asof inner ge';

SELECT 'asof left > string equi key';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p ASOF LEFT JOIN t_ab AS b ON p.ks = b.ks AND p.ts > b.ts SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p ASOF LEFT JOIN t_ab AS b ON p.ks = b.ks AND p.ts > b.ts SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p ASOF LEFT JOIN t_ab AS b ON p.ks = b.ks AND p.ts > b.ts SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4asof left gt string';

SELECT 'asof inner <=';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p ASOF JOIN t_ab AS b ON p.k64 = b.k64 AND p.ts <= b.ts SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p ASOF JOIN t_ab AS b ON p.k64 = b.k64 AND p.ts <= b.ts SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p ASOF JOIN t_ab AS b ON p.k64 = b.k64 AND p.ts <= b.ts SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4asof inner le';

SELECT 'asof inner nullable asof key';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p ASOF JOIN t_ab AS b ON p.k64 = b.k64 AND p.ts >= b.tsnull SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p ASOF JOIN t_ab AS b ON p.k64 = b.k64 AND p.ts >= b.tsnull SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p ASOF JOIN t_ab AS b ON p.k64 = b.k64 AND p.ts >= b.tsnull SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4asof inner nullable';

SELECT 'on filter right side left join';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p LEFT JOIN t_ab AS b ON p.kbig = b.kbig AND b.v % 3 = 0 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p LEFT JOIN t_ab AS b ON p.kbig = b.kbig AND b.v % 3 = 0 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p LEFT JOIN t_ab AS b ON p.kbig = b.kbig AND b.v % 3 = 0 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4filter left rhs';

SELECT 'on filter right side right join (filtered rows are non-joined)';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p RIGHT JOIN t_ab AS b ON p.kbig = b.kbig AND b.v % 3 = 0 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p RIGHT JOIN t_ab AS b ON p.kbig = b.kbig AND b.v % 3 = 0 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p RIGHT JOIN t_ab AS b ON p.kbig = b.kbig AND b.v % 3 = 0 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4filter right rhs';

SELECT 'on filter left side inner join';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p INNER JOIN t_ab AS b ON p.kbig = b.kbig AND p.pv % 5 = 0 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p INNER JOIN t_ab AS b ON p.kbig = b.kbig AND p.pv % 5 = 0 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p INNER JOIN t_ab AS b ON p.kbig = b.kbig AND p.pv % 5 = 0 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4filter inner lhs';

SELECT 'on filter both sides full join';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p FULL JOIN t_ab AS b ON p.kbig = b.kbig AND p.pv % 5 = 0 AND b.v % 3 = 0 SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p FULL JOIN t_ab AS b ON p.kbig = b.kbig AND p.pv % 5 = 0 AND b.v % 3 = 0 SETTINGS join_algorithm = 'parallel_hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p FULL JOIN t_ab AS b ON p.kbig = b.kbig AND p.pv % 5 = 0 AND b.v % 3 = 0 SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4filter full both';

SELECT 'multi-disjunct inner';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p INNER JOIN t_ab AS b ON p.k64 = b.k64 OR p.pv = b.v SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p INNER JOIN t_ab AS b ON p.k64 = b.k64 OR p.pv = b.v SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4or inner';

SELECT 'multi-disjunct left join_use_nulls';
SELECT count(), sum(cityHash64(ifNull(p.pv, 1), ifNull(b.v, 2))) FROM t_ap AS p LEFT JOIN t_ab AS b ON p.k64 = b.k64 OR p.ks = b.ks SETTINGS join_algorithm = 'hash', join_use_nulls = 1;
SELECT count(), sum(cityHash64(ifNull(p.pv, 1), ifNull(b.v, 2))) FROM t_ap AS p LEFT JOIN t_ab AS b ON p.k64 = b.k64 OR p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash', join_use_nulls = 1, log_comment = 'p4or left jun';

SELECT 'multi-disjunct right (per-row flags regime)';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p RIGHT JOIN t_ab AS b ON p.k64 = b.k64 OR p.ks = b.ks SETTINGS join_algorithm = 'hash';
SELECT count(), sum(cityHash64(p.pv, b.v)) FROM t_ap AS p RIGHT JOIN t_ab AS b ON p.k64 = b.k64 OR p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash', log_comment = 'p4or right';

-- ASOF and multi-disjunct run at one partition by design; the ON-filter shapes partition
-- normally. All must engage the algorithm (nonzero leaf rows).
SYSTEM FLUSH LOGS query_log;
SELECT 'partition plans';
SELECT
    log_comment,
    ProfileEvents['PartitionedHashJoinPartitions'],
    ProfileEvents['PartitionedHashJoinLeafRows'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND (log_comment LIKE 'p4asof %' OR log_comment LIKE 'p4or %')
ORDER BY log_comment;
SELECT
    log_comment,
    ProfileEvents['PartitionedHashJoinPartitions'] > 1,
    ProfileEvents['PartitionedHashJoinLeafRows'] > 0,
    ProfileEvents['PartitionedHashJoinHashTableGrowths']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'p4filter %'
ORDER BY log_comment;

DROP TABLE t_ab;
DROP TABLE t_ap;
