-- The old analyzer has its own AST-level predicate pushdown, and the test uses analyzer-only
-- syntax such as (subquery)(c0).
SET enable_analyzer = 1;

-- Parallel replicas replace ReadFromMergeTree with a remote read step, changing the plan shape
-- the EXPLAIN assertions inspect.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_intex_l;
DROP TABLE IF EXISTS t_intex_r;

CREATE TABLE t_intex_l (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_intex_r (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_intex_l SELECT number FROM numbers(1000);
INSERT INTO t_intex_r SELECT number FROM numbers(1000);

-- The pushed key condition must appear on BOTH ReadFromMergeTree branches (count() = 2).
SELECT 'INTERSECT ALL', count() FROM
(EXPLAIN indexes = 1 SELECT a FROM (SELECT a FROM t_intex_l INTERSECT ALL SELECT a FROM t_intex_r) WHERE a = 5)
WHERE explain ILIKE '%Condition:%a in [5, 5]%';

SELECT 'INTERSECT DISTINCT', count() FROM
(EXPLAIN indexes = 1 SELECT a FROM (SELECT a FROM t_intex_l INTERSECT DISTINCT SELECT a FROM t_intex_r) WHERE a = 5)
WHERE explain ILIKE '%Condition:%a in [5, 5]%';

SELECT 'EXCEPT ALL', count() FROM
(EXPLAIN indexes = 1 SELECT a FROM (SELECT a FROM t_intex_l EXCEPT ALL SELECT a FROM t_intex_r) WHERE a = 5)
WHERE explain ILIKE '%Condition:%a in [5, 5]%';

SELECT 'EXCEPT DISTINCT', count() FROM
(EXPLAIN indexes = 1 SELECT a FROM (SELECT a FROM t_intex_l EXCEPT DISTINCT SELECT a FROM t_intex_r) WHERE a = 5)
WHERE explain ILIKE '%Condition:%a in [5, 5]%';

-- The filter must also be removed from above the set operator, not merely duplicated into the
-- branches. Only a top-level step renders with no tree prefix, so 'Filter%' matches a residual
-- filter above IntersectOrExcept and not the branch filters ('|--Filter' / '|  Filter').
SELECT 'no top filter EXCEPT ALL', countIf(explain LIKE 'Filter%') FROM
(EXPLAIN SELECT a FROM (SELECT a FROM t_intex_l EXCEPT ALL SELECT a FROM t_intex_r) WHERE a = 5 SETTINGS query_plan_filter_push_down = 1);
SELECT 'top filter EXCEPT ALL off', countIf(explain LIKE 'Filter%') FROM
(EXPLAIN SELECT a FROM (SELECT a FROM t_intex_l EXCEPT ALL SELECT a FROM t_intex_r) WHERE a = 5 SETTINGS query_plan_filter_push_down = 0);

-- Propagating the condition is not enough: with more than one granule per part, both branches must
-- actually prune down to a single granule, which is the point of the optimization.
DROP TABLE IF EXISTS t_intex_g_l;
DROP TABLE IF EXISTS t_intex_g_r;
-- The granularity settings are pinned because the runner randomizes them, and the assertions below
-- name exact granule counts. The wide-part thresholds must be 0 as well: with adaptive granularity
-- off a non-zero threshold is ignored, and the server logs a <Warning> that fails the test.
CREATE TABLE t_intex_g_l (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
CREATE TABLE t_intex_g_r (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_intex_g_l SELECT number FROM numbers(100000);
INSERT INTO t_intex_g_r SELECT number FROM numbers(100000);
SELECT 'granules on', countIf(explain LIKE '%Granules: 1/13%') FROM
(EXPLAIN indexes = 1 SELECT a FROM (SELECT a FROM t_intex_g_l EXCEPT ALL SELECT a FROM t_intex_g_r) WHERE a = 5 SETTINGS query_plan_filter_push_down = 1);
SELECT 'granules off', countIf(explain LIKE '%Granules: 13/13%') FROM
(EXPLAIN indexes = 1 SELECT a FROM (SELECT a FROM t_intex_g_l EXCEPT ALL SELECT a FROM t_intex_g_r) WHERE a = 5 SETTINGS query_plan_filter_push_down = 0);
DROP TABLE t_intex_g_l;
DROP TABLE t_intex_g_r;

-- Multiplicity must be preserved with the filter pushed down.
DROP TABLE t_intex_l;
DROP TABLE t_intex_r;
CREATE TABLE t_intex_l (a UInt64) ENGINE = Memory;
CREATE TABLE t_intex_r (a UInt64) ENGINE = Memory;
INSERT INTO t_intex_l VALUES (5),(5),(5),(7);
INSERT INTO t_intex_r VALUES (5),(5),(9);

SELECT 'res INTERSECT ALL', a FROM (SELECT a FROM t_intex_l INTERSECT ALL SELECT a FROM t_intex_r) WHERE a = 5 ORDER BY a;
SELECT 'res INTERSECT DISTINCT', a FROM (SELECT a FROM t_intex_l INTERSECT DISTINCT SELECT a FROM t_intex_r) WHERE a = 5 ORDER BY a;
SELECT 'res EXCEPT ALL', a FROM (SELECT a FROM t_intex_l EXCEPT ALL SELECT a FROM t_intex_r) WHERE a = 5 ORDER BY a;
SELECT 'res EXCEPT DISTINCT', a FROM (SELECT a FROM t_intex_l EXCEPT DISTINCT SELECT a FROM t_intex_r) WHERE a = 5 ORDER BY a;

DROP TABLE t_intex_l;
DROP TABLE t_intex_r;

-- A filter consuming a Variant column can throw depending on the alternative a row carries, so it
-- must not be pushed into the branches.
DROP TABLE IF EXISTS t_intex_var_l;
DROP TABLE IF EXISTS t_intex_var_r;
CREATE TABLE t_intex_var_l (c0 Variant(String, UInt64)) ENGINE = Memory SETTINGS allow_experimental_variant_type = 1;
CREATE TABLE t_intex_var_r (c0 Variant(String, UInt64)) ENGINE = Memory SETTINGS allow_experimental_variant_type = 1;
INSERT INTO t_intex_var_l VALUES (0::UInt64), ('x');
INSERT INTO t_intex_var_r VALUES (0::UInt64);
SELECT 'variant except', count() FROM (SELECT c0 FROM t_intex_var_l EXCEPT ALL SELECT c0 FROM t_intex_var_r) WHERE variantElement(c0, 'String') = 'x';
SELECT 'variant intersect', count() FROM (SELECT c0 FROM t_intex_var_l INTERSECT ALL SELECT c0 FROM t_intex_var_r) WHERE variantElement(c0, 'UInt64') = 0;
DROP TABLE t_intex_var_l;
DROP TABLE t_intex_var_r;

-- A Variant column only projected past the set operation is never evaluated, so it must not block
-- the pushdown of a predicate on an ordinary key.
DROP TABLE IF EXISTS t_intex_pv_l;
DROP TABLE IF EXISTS t_intex_pv_r;
CREATE TABLE t_intex_pv_l (a UInt64, v Variant(String, UInt64)) ENGINE = MergeTree ORDER BY a SETTINGS allow_experimental_variant_type = 1;
CREATE TABLE t_intex_pv_r (a UInt64, v Variant(String, UInt64)) ENGINE = MergeTree ORDER BY a SETTINGS allow_experimental_variant_type = 1;
INSERT INTO t_intex_pv_l SELECT number, number::UInt64 FROM numbers(1000);
INSERT INTO t_intex_pv_r SELECT number, number::UInt64 FROM numbers(1000);
SELECT 'proj variant except', count() FROM
(EXPLAIN indexes = 1 SELECT a, v FROM (SELECT a, v FROM t_intex_pv_l EXCEPT ALL SELECT a, v FROM t_intex_pv_r) WHERE a = 5)
WHERE explain ILIKE '%Condition:%a in [5, 5]%';
SELECT 'proj variant intersect', count() FROM
(EXPLAIN indexes = 1 SELECT a, v FROM (SELECT a, v FROM t_intex_pv_l INTERSECT ALL SELECT a, v FROM t_intex_pv_r) WHERE a = 5)
WHERE explain ILIKE '%Condition:%a in [5, 5]%';
DROP TABLE t_intex_pv_l;
DROP TABLE t_intex_pv_r;

-- A deterministic predicate can still throw on some values: the set operation removes the c0 = 0
-- row before the top filter runs, so pushing intDiv into the branches would throw on it.
SELECT 'intdiv except', count() FROM (SELECT c0 FROM ((SELECT 1) EXCEPT ALL SELECT 0)(c0)) WHERE intDiv(1, c0) = 1;
SELECT 'intdiv intersect', count() FROM (SELECT c0 FROM ((SELECT 1) INTERSECT ALL SELECT 0)(c0)) WHERE intDiv(1, c0) = 1;

-- A count() parent needs no branch column, so the pushed filter would project the whole set key
-- away and compute the set over zero columns.
SELECT 'count except', count() FROM (SELECT c0 FROM ((SELECT 'a') EXCEPT ALL (SELECT NULL))(c0)) AS t0 WHERE t0.c0 = t0.c0;
SELECT 'count intersect', count() FROM (SELECT c0 FROM ((SELECT 'a') INTERSECT ALL (SELECT 'a'))(c0)) AS t0 WHERE t0.c0 = t0.c0;
DROP TABLE IF EXISTS t_intex_cnt_l;
DROP TABLE IF EXISTS t_intex_cnt_r;
CREATE TABLE t_intex_cnt_l (a UInt64) ENGINE = Memory;
CREATE TABLE t_intex_cnt_r (a UInt64) ENGINE = Memory;
INSERT INTO t_intex_cnt_l VALUES (5),(5),(7);
INSERT INTO t_intex_cnt_r VALUES (5);
SELECT 'count except mt', count() FROM (SELECT a FROM t_intex_cnt_l EXCEPT ALL SELECT a FROM t_intex_cnt_r) WHERE a = 5;
SELECT 'count intersect mt', count() FROM (SELECT a FROM t_intex_cnt_l INTERSECT ALL SELECT a FROM t_intex_cnt_r) WHERE a = 5;
DROP TABLE t_intex_cnt_l;
DROP TABLE t_intex_cnt_r;

-- Decimal arithmetic raises DECIMAL_OVERFLOW under decimal_check_overflow but does not advertise it
-- via isSuitableForShortCircuitArgumentsExecution, and EXCEPT removes the overflowing row.
DROP TABLE IF EXISTS t_intex_dec_l;
DROP TABLE IF EXISTS t_intex_dec_r;
CREATE TABLE t_intex_dec_l (a Decimal64(0)) ENGINE = Memory;
CREATE TABLE t_intex_dec_r (a Decimal64(0)) ENGINE = Memory;
INSERT INTO t_intex_dec_l VALUES (1),(3000000000);
INSERT INTO t_intex_dec_r VALUES (3000000000);
SELECT 'dec except on', count() FROM (SELECT a FROM t_intex_dec_l EXCEPT ALL SELECT a FROM t_intex_dec_r) WHERE a * toDecimal64(4000000000, 0) = toDecimal64(4000000000, 0) SETTINGS query_plan_filter_push_down = 1;
SELECT 'dec except off', count() FROM (SELECT a FROM t_intex_dec_l EXCEPT ALL SELECT a FROM t_intex_dec_r) WHERE a * toDecimal64(4000000000, 0) = toDecimal64(4000000000, 0) SETTINGS query_plan_filter_push_down = 0;
DROP TABLE t_intex_dec_l;
DROP TABLE t_intex_dec_r;

-- A whitelisted comparison recurses into wrappers, so a Decimal nested in a Tuple can still raise
-- DECIMAL_OVERFLOW on a scale mismatch and must be rejected like a top-level Decimal. This case and
-- the three below must select the set-key column: with a count() parent the set-key guard rejects the
-- pushdown first and the type check is never reached.
DROP TABLE IF EXISTS t_intex_tdec_l;
DROP TABLE IF EXISTS t_intex_tdec_r;
CREATE TABLE t_intex_tdec_l (a Tuple(Decimal64(0))) ENGINE = Memory;
CREATE TABLE t_intex_tdec_r (a Tuple(Decimal64(0))) ENGINE = Memory;
INSERT INTO t_intex_tdec_l VALUES (tuple(toDecimal64(1, 0))), (tuple(toDecimal64(9000000000000000000, 0)));
INSERT INTO t_intex_tdec_r VALUES (tuple(toDecimal64(9000000000000000000, 0)));
SELECT 'tuple dec on', a FROM (SELECT a FROM t_intex_tdec_l EXCEPT ALL SELECT a FROM t_intex_tdec_r) AS t0 WHERE t0.a = tuple(toDecimal64(1, 4)) SETTINGS query_plan_filter_push_down = 1;
SELECT 'tuple dec off', a FROM (SELECT a FROM t_intex_tdec_l EXCEPT ALL SELECT a FROM t_intex_tdec_r) AS t0 WHERE t0.a = tuple(toDecimal64(1, 4)) SETTINGS query_plan_filter_push_down = 0;
DROP TABLE t_intex_tdec_l;
DROP TABLE t_intex_tdec_r;

-- A comparison between mixed types converts first, and converting a String to Date or Enum throws on
-- a value that does not parse, so such a comparison must not be pushed either. These cases select the
-- set-key column (a count() parent hits the set-key guard first) and match no row, so a broken guard
-- shows up as the conversion exception replacing the trailing marker line.
DROP TABLE IF EXISTS t_intex_dat_l;
DROP TABLE IF EXISTS t_intex_dat_r;
CREATE TABLE t_intex_dat_l (a Nullable(Date)) ENGINE = Memory;
CREATE TABLE t_intex_dat_r (a Nullable(Date)) ENGINE = Memory;
INSERT INTO t_intex_dat_l VALUES ('2026-01-01');
INSERT INTO t_intex_dat_r VALUES ('2026-01-01');
SELECT a FROM (SELECT a FROM t_intex_dat_l EXCEPT ALL SELECT a FROM t_intex_dat_r) AS t0 WHERE t0.a = 'bad' SETTINGS query_plan_filter_push_down = 1;
SELECT a FROM (SELECT a FROM t_intex_dat_l EXCEPT ALL SELECT a FROM t_intex_dat_r) AS t0 WHERE t0.a = 'bad' SETTINGS query_plan_filter_push_down = 0;
SELECT 'date str ok';
DROP TABLE t_intex_dat_l;
DROP TABLE t_intex_dat_r;

DROP TABLE IF EXISTS t_intex_enum_l;
DROP TABLE IF EXISTS t_intex_enum_r;
CREATE TABLE t_intex_enum_l (a Nullable(Enum8('x' = 1))) ENGINE = Memory;
CREATE TABLE t_intex_enum_r (a Nullable(Enum8('x' = 1))) ENGINE = Memory;
INSERT INTO t_intex_enum_l VALUES ('x');
INSERT INTO t_intex_enum_r VALUES ('x');
SELECT a FROM (SELECT a FROM t_intex_enum_l EXCEPT ALL SELECT a FROM t_intex_enum_r) AS t0 WHERE t0.a = 'bad' SETTINGS validate_enum_literals_in_operators = 1, query_plan_filter_push_down = 1;
SELECT a FROM (SELECT a FROM t_intex_enum_l EXCEPT ALL SELECT a FROM t_intex_enum_r) AS t0 WHERE t0.a = 'bad' SETTINGS validate_enum_literals_in_operators = 1, query_plan_filter_push_down = 0;
SELECT 'enum str ok';
DROP TABLE t_intex_enum_l;
DROP TABLE t_intex_enum_r;

-- A zero-sized Tuple cannot be compared at all, so its comparison must stay above the set operation
-- where the eliminated rows never reach it.
DROP TABLE IF EXISTS t_intex_et_l;
DROP TABLE IF EXISTS t_intex_et_r;
CREATE TABLE t_intex_et_l (a Nullable(Tuple()), b Nullable(Tuple())) ENGINE = Memory SETTINGS enable_nullable_tuple_type = 1;
CREATE TABLE t_intex_et_r (a Nullable(Tuple()), b Nullable(Tuple())) ENGINE = Memory SETTINGS enable_nullable_tuple_type = 1;
INSERT INTO t_intex_et_l VALUES (tuple(), tuple());
INSERT INTO t_intex_et_r VALUES (tuple(), tuple());
SELECT a, b FROM (SELECT a, b FROM t_intex_et_l EXCEPT ALL SELECT a, b FROM t_intex_et_r) AS t0 WHERE t0.a = t0.b SETTINGS query_plan_filter_push_down = 1;
SELECT a, b FROM (SELECT a, b FROM t_intex_et_l EXCEPT ALL SELECT a, b FROM t_intex_et_r) AS t0 WHERE t0.a = t0.b SETTINGS query_plan_filter_push_down = 0;
SELECT 'empty tuple ok';
DROP TABLE t_intex_et_l;
DROP TABLE t_intex_et_r;

-- Two outputs may alias the same input, which coarsens the set key: (1, 2) and (1, 3) survive
-- EXCEPT ALL, but as (1, 1) and (1, 1) they cancel.
DROP TABLE IF EXISTS t_intex_dup_l;
DROP TABLE IF EXISTS t_intex_dup_r;
CREATE TABLE t_intex_dup_l (a UInt8, b UInt8) ENGINE = Memory;
CREATE TABLE t_intex_dup_r (a UInt8, b UInt8) ENGINE = Memory;
INSERT INTO t_intex_dup_l VALUES (1, 2);
INSERT INTO t_intex_dup_r VALUES (1, 3);
SELECT 'dup input on', x, y FROM (SELECT a AS x, a AS y FROM (SELECT a, b FROM t_intex_dup_l EXCEPT ALL SELECT a, b FROM t_intex_dup_r)) WHERE x > 0 SETTINGS query_plan_filter_push_down = 1;
SELECT 'dup input off', x, y FROM (SELECT a AS x, a AS y FROM (SELECT a, b FROM t_intex_dup_l EXCEPT ALL SELECT a, b FROM t_intex_dup_r)) WHERE x > 0 SETTINGS query_plan_filter_push_down = 0;
DROP TABLE t_intex_dup_l;
DROP TABLE t_intex_dup_r;

-- A parent reusing the predicate column leaves a filter output of a single same-typed UInt8, so
-- pushing it would feed x > 0 into the set instead of x.
DROP TABLE IF EXISTS t_intex_reuse_l;
DROP TABLE IF EXISTS t_intex_reuse_r;
CREATE TABLE t_intex_reuse_l (x UInt8) ENGINE = Memory;
CREATE TABLE t_intex_reuse_r (x UInt8) ENGINE = Memory;
INSERT INTO t_intex_reuse_l VALUES (2),(2),(5);
INSERT INTO t_intex_reuse_r VALUES (3);
SELECT 'reuse on', count() FROM (SELECT (x > 0) AS p FROM (SELECT x FROM t_intex_reuse_l EXCEPT ALL SELECT x FROM t_intex_reuse_r) WHERE x > 0) SETTINGS query_plan_filter_push_down = 1;
SELECT 'reuse off', count() FROM (SELECT (x > 0) AS p FROM (SELECT x FROM t_intex_reuse_l EXCEPT ALL SELECT x FROM t_intex_reuse_r) WHERE x > 0) SETTINGS query_plan_filter_push_down = 0;
DROP TABLE t_intex_reuse_l;
DROP TABLE t_intex_reuse_r;

-- One branch constant-folds its set-key column to a Const while the sibling stays full. The
-- plan-time header check must match the contract updatePipeline reconciles at runtime.
SELECT 'block mismatch', count() FROM (SELECT DISTINCT x FROM (SELECT DISTINCT NULL AS x INTERSECT ALL SELECT DISTINCT NULL AS x GROUP BY NULL)) AS t0 WHERE t0.x = t0.x SETTINGS query_plan_filter_push_down = 1;
SELECT 'block mismatch off', count() FROM (SELECT DISTINCT x FROM (SELECT DISTINCT NULL AS x INTERSECT ALL SELECT DISTINCT NULL AS x GROUP BY NULL)) AS t0 WHERE t0.x = t0.x SETTINGS query_plan_filter_push_down = 0;

-- IntersectOrExceptTransform uniformizes only the main ports, so a WITH TOTALS branch whose main port
-- constant-folds leaves the outer DISTINCT comparing a Const main port against a full totals one.
SELECT 'totals except', count() FROM (SELECT DISTINCT x FROM (SELECT DISTINCT NULL AS x GROUP BY 1, NULL EXCEPT ALL SELECT DISTINCT NULL AS x GROUP BY 'z', NULL WITH TOTALS)) AS t0 WHERE t0.x = t0.x SETTINGS query_plan_filter_push_down = 1;
SELECT 'totals except off', count() FROM (SELECT DISTINCT x FROM (SELECT DISTINCT NULL AS x GROUP BY 1, NULL EXCEPT ALL SELECT DISTINCT NULL AS x GROUP BY 'z', NULL WITH TOTALS)) AS t0 WHERE t0.x = t0.x SETTINGS query_plan_filter_push_down = 0;
SELECT 'totals intersect', count() FROM (SELECT DISTINCT x FROM (SELECT DISTINCT NULL AS x GROUP BY 1, NULL INTERSECT ALL SELECT DISTINCT NULL AS x GROUP BY 'z', NULL WITH TOTALS)) AS t0 WHERE t0.x = t0.x SETTINGS query_plan_filter_push_down = 1;
SELECT 'totals intersect off', count() FROM (SELECT DISTINCT x FROM (SELECT DISTINCT NULL AS x GROUP BY 1, NULL INTERSECT ALL SELECT DISTINCT NULL AS x GROUP BY 'z', NULL WITH TOTALS)) AS t0 WHERE t0.x = t0.x SETTINGS query_plan_filter_push_down = 0;
