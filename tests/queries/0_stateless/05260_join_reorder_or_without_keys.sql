-- An equality in JOIN ON next to ORs without equalities must stay a join condition that join
-- reordering can use (issue #122510): `x` is joined with `y` on `x.cid = y.c2` first, and with `p`
-- on the two-valued key `x.g = p.c3` last, and `coalesce(y.c4, 0) = 1` is read as PREWHERE of `t3`.
-- Unlike in the issue, `t3.c5` and `t3.c6` are not all NULL, so both branches of each OR decide the count.

-- The chosen join order and the count depend on these settings, which the test runner randomizes.
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET allow_general_join_planning = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = false;
SET use_hash_table_stats_for_join_reordering = 0;
SET session_timezone = 'UTC';
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t4;

CREATE TABLE t1 (c1 UInt64, c2 UInt64, c3 Nullable(DateTime), c4 Nullable(DateTime), c5 String) ENGINE = MergeTree ORDER BY c1 SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t2 (c1 UInt64, c2 String) ENGINE = MergeTree ORDER BY c1 SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t3 (c1 UInt64, c2 UInt64, c3 UInt64, c4 Nullable(UInt8), c5 Nullable(DateTime), c6 Nullable(DateTime)) ENGINE = MergeTree ORDER BY c1 SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t4 (c1 UInt64, c2 UInt64, c3 String, c4 String) ENGINE = MergeTree ORDER BY c1 SETTINGS auto_statistics_types = 'basic, uniq_v2';

INSERT INTO t1 SELECT number, number % 5000, toDateTime('2024-01-01 00:00:00') + (number % 600) * 86400, if(number % 3 = 0, NULL, toDateTime('2025-06-01 00:00:00') + (number % 200) * 86400), if(number % 5 = 0, 'x', 'a') FROM numbers(10000);
INSERT INTO t2 SELECT number, if(number % 2 = 0, 'o1', 'o2') FROM numbers(5000);
INSERT INTO t3 SELECT number, number % 10000, cityHash64(number) % 250, if(number < 10000, 1, 0), if(number % 3 = 0, NULL, toDateTime('2024-01-01 00:00:00') + (number % 1000) * 86400), if(number % 7 = 0, NULL, toDateTime('2024-01-01 00:00:00') + (number % 700) * 86400) FROM numbers(20000);
INSERT INTO t4 SELECT number, number, if(number % 2 = 0, 'g1', 'g2'), 's' FROM numbers(250);

SELECT replaceRegexpAll(replaceRegexpAll(explain, '^[^a-z(]+', ''), '\\[[^\\]]*\\]', '')
FROM
(
    EXPLAIN
    WITH q1 AS (SELECT coalesce(toStartOfMonth(min(toDate(c3))), toStartOfMonth(toDate('2026-08-19'))) AS m0, toStartOfMonth(toDate('2026-08-19')) AS m1 FROM t1 WHERE c5 IN ('a', 'b') AND c3 IS NOT NULL), q2 AS (SELECT addMonths(q1.m0, o) AS ms, addMonths(q1.m0, o + 1) AS nms, toUInt32(formatDateTime(addMonths(q1.m0, o), '%Y%m')) AS mid FROM q1 ARRAY JOIN range(toUInt32(dateDiff('month', q1.m0, q1.m1))) AS o), q3 AS (SELECT c1 AS sid, caseWithExpression(c2, 'o1', 'g1', 'o2', 'g2', NULL) AS g FROM t2), q4 AS (SELECT k.c1 AS cid, k.c2 AS sid, q3.g, q2.mid, q2.ms, q2.nms, row_number() OVER (PARTITION BY k.c2, q2.mid ORDER BY k.c3 ASC, k.c1 ASC) AS rk FROM t1 AS k INNER JOIN q3 ON k.c2 = q3.sid INNER JOIN q2 ON toDate(k.c3) < q2.nms AND (k.c4 IS NULL OR toDate(k.c4) >= q2.ms) WHERE k.c5 IN ('a', 'b') AND k.c3 IS NOT NULL AND q3.g IS NOT NULL), q5 AS (SELECT * FROM q4 WHERE rk = 1) SELECT count() FROM q5 AS x INNER JOIN t3 AS y ON x.cid = y.c2 AND coalesce(y.c4, 0) = 1 AND (y.c5 IS NULL OR toDate(y.c5) < x.nms) AND (y.c6 IS NULL OR toDate(y.c6) >= x.ms) INNER JOIN t4 AS p ON y.c3 = p.c2 AND x.g = p.c3 WHERE p.c4 = 's'
)
WHERE explain LIKE '%x ⋈%';

SELECT count()
FROM
(
    EXPLAIN
    WITH q1 AS (SELECT coalesce(toStartOfMonth(min(toDate(c3))), toStartOfMonth(toDate('2026-08-19'))) AS m0, toStartOfMonth(toDate('2026-08-19')) AS m1 FROM t1 WHERE c5 IN ('a', 'b') AND c3 IS NOT NULL), q2 AS (SELECT addMonths(q1.m0, o) AS ms, addMonths(q1.m0, o + 1) AS nms, toUInt32(formatDateTime(addMonths(q1.m0, o), '%Y%m')) AS mid FROM q1 ARRAY JOIN range(toUInt32(dateDiff('month', q1.m0, q1.m1))) AS o), q3 AS (SELECT c1 AS sid, caseWithExpression(c2, 'o1', 'g1', 'o2', 'g2', NULL) AS g FROM t2), q4 AS (SELECT k.c1 AS cid, k.c2 AS sid, q3.g, q2.mid, q2.ms, q2.nms, row_number() OVER (PARTITION BY k.c2, q2.mid ORDER BY k.c3 ASC, k.c1 ASC) AS rk FROM t1 AS k INNER JOIN q3 ON k.c2 = q3.sid INNER JOIN q2 ON toDate(k.c3) < q2.nms AND (k.c4 IS NULL OR toDate(k.c4) >= q2.ms) WHERE k.c5 IN ('a', 'b') AND k.c3 IS NOT NULL AND q3.g IS NOT NULL), q5 AS (SELECT * FROM q4 WHERE rk = 1) SELECT count() FROM q5 AS x INNER JOIN t3 AS y ON x.cid = y.c2 AND coalesce(y.c4, 0) = 1 AND (y.c5 IS NULL OR toDate(y.c5) < x.nms) AND (y.c6 IS NULL OR toDate(y.c6) >= x.ms) INNER JOIN t4 AS p ON y.c3 = p.c2 AND x.g = p.c3 WHERE p.c4 = 's'
)
WHERE explain LIKE '%Prewhere filter column:%coalesce(c4, 0) = 1%';

WITH q1 AS (SELECT coalesce(toStartOfMonth(min(toDate(c3))), toStartOfMonth(toDate('2026-08-19'))) AS m0, toStartOfMonth(toDate('2026-08-19')) AS m1 FROM t1 WHERE c5 IN ('a', 'b') AND c3 IS NOT NULL), q2 AS (SELECT addMonths(q1.m0, o) AS ms, addMonths(q1.m0, o + 1) AS nms, toUInt32(formatDateTime(addMonths(q1.m0, o), '%Y%m')) AS mid FROM q1 ARRAY JOIN range(toUInt32(dateDiff('month', q1.m0, q1.m1))) AS o), q3 AS (SELECT c1 AS sid, caseWithExpression(c2, 'o1', 'g1', 'o2', 'g2', NULL) AS g FROM t2), q4 AS (SELECT k.c1 AS cid, k.c2 AS sid, q3.g, q2.mid, q2.ms, q2.nms, row_number() OVER (PARTITION BY k.c2, q2.mid ORDER BY k.c3 ASC, k.c1 ASC) AS rk FROM t1 AS k INNER JOIN q3 ON k.c2 = q3.sid INNER JOIN q2 ON toDate(k.c3) < q2.nms AND (k.c4 IS NULL OR toDate(k.c4) >= q2.ms) WHERE k.c5 IN ('a', 'b') AND k.c3 IS NOT NULL AND q3.g IS NOT NULL), q5 AS (SELECT * FROM q4 WHERE rk = 1) SELECT count() FROM q5 AS x INNER JOIN t3 AS y ON x.cid = y.c2 AND coalesce(y.c4, 0) = 1 AND (y.c5 IS NULL OR toDate(y.c5) < x.nms) AND (y.c6 IS NULL OR toDate(y.c6) >= x.ms) INNER JOIN t4 AS p ON y.c3 = p.c2 AND x.g = p.c3 WHERE p.c4 = 's';

-- An OR of conditions on one table each keeps working in an outer join with join_algorithm = 'auto'.
SELECT count(), sum(b.c1) FROM t4 AS a LEFT JOIN t4 AS b ON a.c1 = b.c2 AND (a.c1 > 100 OR b.c1 > 200) SETTINGS join_algorithm = 'auto';

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
