-- After the outer join to t2 is converted to inner and the joins are reordered, the join of the fact table with
-- the filtered dimension table must stay on the probe side of the outer joins with the other dimension tables.
-- The reproducer of https://github.com/ClickHouse/ClickHouse/issues/122301, scaled down.

SET explain_query_plan_default = 'legacy';
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET use_hash_table_stats_for_join_reordering = 0;
SET query_plan_join_swap_table = 'auto';
SET query_plan_convert_outer_join_to_inner_join = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
-- Pin the other settings randomized in CI that change the join graph or the plan shape (to their defaults).
SET enable_join_transitive_predicates = 1;
SET use_join_disjunctions_push_down = 1;
SET query_plan_join_shard_by_pk_ranges = 0;
SET query_plan_merge_filter_into_join_condition = 1;
SET query_plan_merge_filters = 1;
SET query_plan_remove_unused_columns = 1;

-- No column statistics on the fact table, so the NDV of its join keys is unknown.
CREATE TABLE t1 (c1 Date, c2 String, c3 Int64, c4 Int64, c5 String, c6 Int64, c7 String, c8 String, c9 Decimal(38, 4), c10 String, c11 Int64) ENGINE = MergeTree PARTITION BY (c5, toYYYYMM(c1)) ORDER BY (c1, c2, c5) SETTINGS auto_statistics_types = '';
CREATE TABLE t2 (c5 String, c3 Int32, c12 Nullable(String)) ENGINE = MergeTree ORDER BY (c5, c3) SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t3 (c5 String, c4 Float64, c13 String) ENGINE = MergeTree ORDER BY (c5, c4) SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t4 (c5 String, c6 Int32, c14 Nullable(String)) ENGINE = MergeTree ORDER BY (c5, c6) SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t5 (c15 Nullable(String), c16 Nullable(String), c6 Nullable(Int64)) ENGINE = MergeTree ORDER BY c15 SETTINGS allow_nullable_key = 1, auto_statistics_types = 'basic, uniq_v2';

INSERT INTO t1 SELECT toDate('2026-08-01') + (number % 31), toString(intHash64(number * 3) % 400000), toInt64(intHash64(number + 7) % 280), toInt64(intHash64(number + 1) % 3500), if(number % 10 < 8, 'AE', 'SA'), toInt64(intHash64(number + 2) % 420 + 1), ['o1','o2','o3','o4'][number % 4 + 1], concat('p', toString(intHash64(number + 9) % 12)), toDecimal128(intHash64(number + 5) % 1000000, 4), toString(intHash64(number + 11) % 8), toInt64(intHash64(number + 13) % 5000) FROM numbers(80000);
INSERT INTO t1 SELECT toDate('2025-09-01') + (number % 334), toString(intHash64(number * 3) % 400000), toInt64(intHash64(number + 7) % 280), toInt64(intHash64(number + 1) % 3500), if(number % 10 < 8, 'AE', 'SA'), toInt64(intHash64(number + 2) % 420 + 1), ['o1','o2','o3','o4'][number % 4 + 1], concat('p', toString(intHash64(number + 9) % 12)), toDecimal128(intHash64(number + 5) % 1000000, 4), toString(intHash64(number + 11) % 8), toInt64(intHash64(number + 13) % 5000) FROM numbers(160000);
INSERT INTO t2 SELECT if(number < 280, 'AE', 'SA'), toInt32(number % 280), if(number % 10 = 0, concat('X', toString(number % 50)), concat('n', toString(number % 900))) FROM numbers(286);
INSERT INTO t3 SELECT if(number < 3500, 'AE', 'SA'), toFloat64(number % 3500), concat('a', toString(number)) FROM numbers(4645);
INSERT INTO t4 SELECT if(number < 420, 'AE', 'SA'), toInt32(number % 420 + 1), concat('k', toString(number % 20)) FROM numbers(686);
INSERT INTO t5 SELECT toString(number), concat('h', toString(number % 900)), toInt64(intHash64(number) % 420) FROM numbers(32000);

CREATE VIEW v1 AS SELECT A.c1 AS c1, A.c2 AS c2, A.c5 AS c5, B.c12 AS c12, D.c13 AS c13, coalesce(multiIf(L.c14 = '', NULL, L.c14), 'z') AS c14, E.c17 AS c17, A.c7 AS c7, A.c8 AS c8, A.c9 AS c9, A.c10 AS c10, A.c11 AS c11 FROM t1 AS A LEFT JOIN t2 AS B ON (A.c3 = B.c3) AND (A.c5 = B.c5) LEFT JOIN t3 AS D ON (A.c4 = CAST(D.c4, 'Int64')) AND (A.c5 = D.c5) LEFT JOIN t4 AS L ON (A.c6 = L.c6) AND (A.c5 = L.c5) LEFT JOIN (SELECT any(c16) AS c17, c6, 'AE' AS c5 FROM t5 GROUP BY c6) AS E ON (E.c5 = A.c5) AND (E.c6 = A.c6);

CREATE VIEW v2 AS WITH r AS ( SELECT c7 AS a, c8 AS b, c10 AS e, toDate(c1) AS d, uniqExact(c2) AS x1, quantileExactIf(0.5)(c9, NOT (c9 IS NULL)) AS x2, uniqExactIf(c2, NOT (c9 IS NULL)) AS x3, sum(CAST(c11, 'Nullable(Float64)')) AS x4, count(CAST(c11, 'Nullable(Float64)')) AS x5 FROM v1 WHERE (NOT (lower(c12) LIKE lower('X%'))) AND ((c1 >= dateTrunc('month', subtractMonths(toDate('2026-09-04'), 1))) AND (c1 < dateTrunc('month', toDate('2026-09-04')))) GROUP BY a, b, e, d ) SELECT a, b, e, round(CAST(sum(x1), 'Nullable(Float64)') / CAST(nullIf(dateDiff('day', dateTrunc('month', subtractMonths(toDate('2026-09-04'), 1)), dateTrunc('month', toDate('2026-09-04'))), 0), 'Nullable(Float64)')) AS y1, CAST(sum(CAST(x2, 'Nullable(Float64)') * CAST(x3, 'Nullable(Float64)')), 'Nullable(Float64)') / CAST(nullIf(sum(x3), 0), 'Nullable(Float64)') AS y2, CAST(sum(x4), 'Nullable(Float64)') / CAST(nullIf(sum(x5), 0), 'Nullable(Float64)') AS y3 FROM r GROUP BY a, b, e ORDER BY a ASC, y1 DESC, b ASC, e ASC LIMIT 10001;

-- Expected: the join with t2 is INNER and the three dimension joins stay LEFT; a RIGHT join means the hash table
-- is built on the fact table join.
SELECT countIf(explain ILIKE '%Type: INNER%'), countIf(explain ILIKE '%Type: LEFT%'), countIf(explain ILIKE '%Type: RIGHT%')
FROM (EXPLAIN actions = 1 SELECT * FROM v2);

-- Forcing the swap must flip the three outer joins; otherwise the optimizer skipped this join graph and the check
-- above proves nothing.
SET query_plan_join_swap_table = 'true';
SELECT countIf(explain ILIKE '%Type: INNER%'), countIf(explain ILIKE '%Type: LEFT%'), countIf(explain ILIKE '%Type: RIGHT%')
FROM (EXPLAIN actions = 1 SELECT * FROM v2);

DROP VIEW v2;
DROP VIEW v1;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
DROP TABLE t4;
DROP TABLE t5;
