-- ASOF joins, ON-clause filter conditions on one side, and multi-disjunct ON (`OR` of key sets)
-- under `partitioned_hash`, against `hash` and `parallel_hash`. ASOF and multi-disjunct builds use
-- one partition by design. The query log check at the end asserts that. The ON-filter builds
-- partition as usual. Right rows the filter removed still appear as non-joined rows of RIGHT and
-- FULL joins. Mixed non-equi ON conditions fall back at plan time (04926 asserts that).

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
-- The runner randomizes `max_bytes_before_external_join`; any non-zero spill budget would wrap the join
-- in `SpillingHashJoin`.
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

SELECT 'asof inner >=', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p ASOF JOIN t_ab AS b ON p.k64 = b.k64 AND p.ts >= b.ts SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p ASOF JOIN t_ab AS b ON p.k64 = b.k64 AND p.ts >= b.ts SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p ASOF JOIN t_ab AS b ON p.k64 = b.k64 AND p.ts >= b.ts SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04929 asof inner ge';

SELECT 'asof left > string equi key', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p ASOF LEFT JOIN t_ab AS b ON p.ks = b.ks AND p.ts > b.ts SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p ASOF LEFT JOIN t_ab AS b ON p.ks = b.ks AND p.ts > b.ts SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p ASOF LEFT JOIN t_ab AS b ON p.ks = b.ks AND p.ts > b.ts SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04929 asof left gt string';

SELECT 'asof inner <=', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p ASOF JOIN t_ab AS b ON p.k64 = b.k64 AND p.ts <= b.ts SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p ASOF JOIN t_ab AS b ON p.k64 = b.k64 AND p.ts <= b.ts SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p ASOF JOIN t_ab AS b ON p.k64 = b.k64 AND p.ts <= b.ts SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04929 asof inner le';

SELECT 'asof inner nullable asof key', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p ASOF JOIN t_ab AS b ON p.k64 = b.k64 AND p.ts >= b.tsnull SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p ASOF JOIN t_ab AS b ON p.k64 = b.k64 AND p.ts >= b.tsnull SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p ASOF JOIN t_ab AS b ON p.k64 = b.k64 AND p.ts >= b.tsnull SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04929 asof inner nullable';

SELECT 'on filter right side left join', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p LEFT JOIN t_ab AS b ON p.kbig = b.kbig AND b.v % 3 = 0 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p LEFT JOIN t_ab AS b ON p.kbig = b.kbig AND b.v % 3 = 0 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p LEFT JOIN t_ab AS b ON p.kbig = b.kbig AND b.v % 3 = 0 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04929 filter left rhs';

SELECT 'on filter right side right join (filtered rows are non-joined)', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p RIGHT JOIN t_ab AS b ON p.kbig = b.kbig AND b.v % 3 = 0 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p RIGHT JOIN t_ab AS b ON p.kbig = b.kbig AND b.v % 3 = 0 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p RIGHT JOIN t_ab AS b ON p.kbig = b.kbig AND b.v % 3 = 0 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04929 filter right rhs';

SELECT 'on filter left side inner join', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p INNER JOIN t_ab AS b ON p.kbig = b.kbig AND p.pv % 5 = 0 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p INNER JOIN t_ab AS b ON p.kbig = b.kbig AND p.pv % 5 = 0 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p INNER JOIN t_ab AS b ON p.kbig = b.kbig AND p.pv % 5 = 0 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04929 filter inner lhs';

SELECT 'on filter both sides full join', h.1, h = ph, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p FULL JOIN t_ab AS b ON p.kbig = b.kbig AND p.pv % 5 = 0 AND b.v % 3 = 0 SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p FULL JOIN t_ab AS b ON p.kbig = b.kbig AND p.pv % 5 = 0 AND b.v % 3 = 0 SETTINGS join_algorithm = 'parallel_hash') AS ph,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p FULL JOIN t_ab AS b ON p.kbig = b.kbig AND p.pv % 5 = 0 AND b.v % 3 = 0 SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04929 filter full both';

SELECT 'multi-disjunct inner', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p INNER JOIN t_ab AS b ON p.k64 = b.k64 OR p.pv = b.v SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p INNER JOIN t_ab AS b ON p.k64 = b.k64 OR p.pv = b.v SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04929 or inner';

SELECT 'multi-disjunct left join_use_nulls', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(ifNull(p.pv, 1), ifNull(b.v, 2)))) FROM t_ap AS p LEFT JOIN t_ab AS b ON p.k64 = b.k64 OR p.ks = b.ks SETTINGS join_algorithm = 'hash', join_use_nulls = 1) AS h,
    (SELECT (count(), sum(cityHash64(ifNull(p.pv, 1), ifNull(b.v, 2)))) FROM t_ap AS p LEFT JOIN t_ab AS b ON p.k64 = b.k64 OR p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash', join_use_nulls = 1) AS pa)
SETTINGS log_comment = '04929 or left join_use_nulls';

SELECT 'multi-disjunct right (used flags kept per row)', h.1, h = pa FROM (SELECT
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p RIGHT JOIN t_ab AS b ON p.k64 = b.k64 OR p.ks = b.ks SETTINGS join_algorithm = 'hash') AS h,
    (SELECT (count(), sum(cityHash64(p.pv, b.v))) FROM t_ap AS p RIGHT JOIN t_ab AS b ON p.k64 = b.k64 OR p.ks = b.ks SETTINGS join_algorithm = 'partitioned_hash') AS pa)
SETTINGS log_comment = '04929 or right';

-- ASOF and multi-disjunct builds use one partition by design; the ON-filter builds partition as usual.
-- All must have inserted rows through the partitioned build.
SYSTEM FLUSH LOGS query_log;

SELECT 'partition plans';
SELECT
    log_comment,
    ProfileEvents['HashJoinPartitions'],
    ProfileEvents['HashJoinInsertedRows'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND (log_comment LIKE '04929 asof %' OR log_comment LIKE '04929 or %')
ORDER BY log_comment;
SELECT
    log_comment,
    ProfileEvents['HashJoinPartitions'] > 1,
    ProfileEvents['HashJoinInsertedRows'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '04929 filter %'
ORDER BY log_comment;

DROP TABLE t_ab;
DROP TABLE t_ap;
