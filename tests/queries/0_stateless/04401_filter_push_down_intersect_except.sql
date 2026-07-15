-- Filter above INTERSECT/EXCEPT (ALL and DISTINCT) must be pushed into every input
-- branch, exactly as for UNION ALL, so each input prunes with its own index.
-- https://github.com/ClickHouse/ClickHouse/issues/110113

-- This targets the analyzer query-plan filter pushdown (query_plan_filter_push_down into
-- IntersectOrExceptStep) and uses analyzer-only syntax such as (subquery)(c0). The old
-- analyzer has its own AST-level predicate pushdown, so pin the analyzer on to keep the test
-- stable under the old-analyzer CI runner.
SET enable_analyzer = 1;

-- Parallel replicas replace ReadFromMergeTree with a remote read step, changing the
-- plan shape the EXPLAIN assertions below inspect; pin it off for a stable local plan.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_intex_l;
DROP TABLE IF EXISTS t_intex_r;

CREATE TABLE t_intex_l (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_intex_r (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_intex_l SELECT number FROM numbers(1000);
INSERT INTO t_intex_r SELECT number FROM numbers(1000);

-- Each set operator: the pushed key condition (a in [5, 5]) must appear on BOTH
-- ReadFromMergeTree branches (count() = 2), proving the filter reached each input.

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

-- The filter must also be REMOVED from above the set operator (not merely duplicated into the
-- branches), exactly as for UNION ALL. A top-level plan step renders with no tree prefix, so a
-- residual Filter above IntersectOrExcept matches 'Filter%'; branch filters render '|--Filter'/'|  Filter'
-- and do not. With pushdown on there must be 0 top-level Filter steps; with it off there is 1.
SELECT 'no top filter EXCEPT ALL', countIf(explain LIKE 'Filter%') FROM
(EXPLAIN SELECT a FROM (SELECT a FROM t_intex_l EXCEPT ALL SELECT a FROM t_intex_r) WHERE a = 5 SETTINGS query_plan_filter_push_down = 1);
SELECT 'top filter EXCEPT ALL off', countIf(explain LIKE 'Filter%') FROM
(EXPLAIN SELECT a FROM (SELECT a FROM t_intex_l EXCEPT ALL SELECT a FROM t_intex_r) WHERE a = 5 SETTINGS query_plan_filter_push_down = 0);

-- Correctness: multiplicity/semantics preserved with the filter pushed down.
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

-- A filter over a Variant column can throw at runtime depending on the concrete alternative a row
-- carries. INTERSECT/EXCEPT eliminate rows, so pushing such a filter into the branches would evaluate
-- it on rows the set op removes and surface an error the unoptimized plan never produces. The pushdown
-- must be skipped for these columns. https://github.com/ClickHouse/ClickHouse/issues/110113
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

-- The guard rejects a Variant/Dynamic value only when it is CONSUMED by a filter function, not when
-- it is merely projected through to the output. A Variant column carried past the set operation while
-- the predicate filters on an ordinary key (a = 5) cannot raise an eliminated-row exception, so the
-- key-condition pushdown must still reach both branches. https://github.com/ClickHouse/ClickHouse/issues/110113
DROP TABLE IF EXISTS t_intex_pv_l;
DROP TABLE IF EXISTS t_intex_pv_r;
CREATE TABLE t_intex_pv_l (a UInt64, v Variant(String, UInt64)) ENGINE = MergeTree ORDER BY a SETTINGS allow_experimental_variant_type = 1;
CREATE TABLE t_intex_pv_r (a UInt64, v Variant(String, UInt64)) ENGINE = MergeTree ORDER BY a SETTINGS allow_experimental_variant_type = 1;
INSERT INTO t_intex_pv_l SELECT number, number::UInt64 FROM numbers(1000);
INSERT INTO t_intex_pv_r SELECT number, number::UInt64 FROM numbers(1000);
-- Variant only projected, predicate a = 5 (safe): pushdown fires on both branches (count = 2).
SELECT 'proj variant except', count() FROM
(EXPLAIN indexes = 1 SELECT a, v FROM (SELECT a, v FROM t_intex_pv_l EXCEPT ALL SELECT a, v FROM t_intex_pv_r) WHERE a = 5)
WHERE explain ILIKE '%Condition:%a in [5, 5]%';
SELECT 'proj variant intersect', count() FROM
(EXPLAIN indexes = 1 SELECT a, v FROM (SELECT a, v FROM t_intex_pv_l INTERSECT ALL SELECT a, v FROM t_intex_pv_r) WHERE a = 5)
WHERE explain ILIKE '%Condition:%a in [5, 5]%';
DROP TABLE t_intex_pv_l;
DROP TABLE t_intex_pv_r;

-- A deterministic predicate can still throw on some values: intDiv(1, c0) throws on a c0 = 0 row.
-- INTERSECT/EXCEPT remove that row before the top filter runs, so without the optimization the
-- query returns 1. Pushing the filter into the branches would evaluate intDiv on the eliminated
-- 0 row and throw ILLEGAL_DIVISION. The pushdown must be skipped for throwing predicates.
SELECT 'intdiv except', count() FROM (SELECT c0 FROM ((SELECT 1) EXCEPT ALL SELECT 0)(c0)) WHERE intDiv(1, c0) = 1;
SELECT 'intdiv intersect', count() FROM (SELECT c0 FROM ((SELECT 1) INTERSECT ALL SELECT 0)(c0)) WHERE intDiv(1, c0) = 1;

-- INTERSECT/EXCEPT compare whole rows: the entire branch header is the set key. When the parent
-- needs no branch column (count()) the pushed filter projects them all away, so the set would be
-- computed over zero columns: a wrong result and a num_srcs > 0 abort. The pushdown must be skipped
-- unless the set key is preserved. https://github.com/ClickHouse/ClickHouse/issues/110113
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

-- Decimal plus/minus/multiply raise DECIMAL_OVERFLOW under decimal_check_overflow (default on) but
-- do not advertise it via isSuitableForShortCircuitArgumentsExecution, so the predicate-cannot-throw
-- guard must not rely on that signal. The overflow row (3000000000) is removed by EXCEPT before the
-- top filter runs, so the query returns 1; pushing the multiply into the branch would overflow on the
-- eliminated row. Both directions must agree.
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

-- IntersectOrExcept compares whole branch rows, so a pushed filter must feed the original branch
-- columns into the set, not a computed predicate result. When a parent reuses the predicate column
-- (SELECT x > 0 ... WHERE x > 0) the filter output can be a single same-typed UInt8 (x > 0); pushing
-- it would replace the set key x with x > 0 and change the result. The pushdown must be skipped.
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

-- One branch constant-folds its column to a Const while the sibling keeps a full column (here NULL
-- folds to Const(Nullable(Nothing)) vs a GROUP BY branch that stays full). Pushing the top filter into
-- each branch makes the plan-time header check compare the two branches, and the strict structural
-- check aborted with "Block structure mismatch in IntersectOrExceptStep stream" on the Const/full
-- difference, even though updatePipeline reconciles it at runtime. The plan-time check must match that
-- relaxed contract. https://github.com/ClickHouse/ClickHouse/issues/110113
SELECT 'block mismatch', count() FROM (SELECT DISTINCT x FROM (SELECT DISTINCT NULL AS x INTERSECT ALL SELECT DISTINCT NULL AS x GROUP BY NULL)) AS t0 WHERE t0.x = t0.x SETTINGS query_plan_filter_push_down = 1;
SELECT 'block mismatch off', count() FROM (SELECT DISTINCT x FROM (SELECT DISTINCT NULL AS x INTERSECT ALL SELECT DISTINCT NULL AS x GROUP BY NULL)) AS t0 WHERE t0.x = t0.x SETTINGS query_plan_filter_push_down = 0;

-- A branch with WITH TOTALS emits a totals port. IntersectOrExceptTransform consumes only the main
-- ports and uniformizes them to its output header, but the totals port bypasses the transform. When
-- a branch constant-folds a set-key column (NULL AS x), pushing the top filter into that branch left
-- the main port Const while the totals port stayed full, so a downstream Main-only transform (the
-- outer DISTINCT) compared a Const main port against a full totals port and aborted with a "Block
-- structure mismatch in QueryPipeline stream". The pushdown must be skipped when a branch emits
-- totals. Both directions must agree. https://github.com/ClickHouse/ClickHouse/issues/110113
SELECT 'totals except', count() FROM (SELECT DISTINCT x FROM (SELECT DISTINCT NULL AS x GROUP BY 1, NULL EXCEPT ALL SELECT DISTINCT NULL AS x GROUP BY 'z', NULL WITH TOTALS)) AS t0 WHERE t0.x = t0.x SETTINGS query_plan_filter_push_down = 1;
SELECT 'totals except off', count() FROM (SELECT DISTINCT x FROM (SELECT DISTINCT NULL AS x GROUP BY 1, NULL EXCEPT ALL SELECT DISTINCT NULL AS x GROUP BY 'z', NULL WITH TOTALS)) AS t0 WHERE t0.x = t0.x SETTINGS query_plan_filter_push_down = 0;
SELECT 'totals intersect', count() FROM (SELECT DISTINCT x FROM (SELECT DISTINCT NULL AS x GROUP BY 1, NULL INTERSECT ALL SELECT DISTINCT NULL AS x GROUP BY 'z', NULL WITH TOTALS)) AS t0 WHERE t0.x = t0.x SETTINGS query_plan_filter_push_down = 1;
SELECT 'totals intersect off', count() FROM (SELECT DISTINCT x FROM (SELECT DISTINCT NULL AS x GROUP BY 1, NULL INTERSECT ALL SELECT DISTINCT NULL AS x GROUP BY 'z', NULL WITH TOTALS)) AS t0 WHERE t0.x = t0.x SETTINGS query_plan_filter_push_down = 0;
