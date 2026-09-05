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

-- Filtering each branch independently selects different rows in each unless the predicate is
-- deterministic within the query, so a rand64 predicate must stay above the set operation.
SELECT 'nondeterministic top filter', countIf(explain LIKE 'Filter%') FROM
(EXPLAIN SELECT a FROM (SELECT a FROM t_intex_l EXCEPT ALL SELECT a FROM t_intex_r) WHERE rand64() % 2 = 0);

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
-- Both results are 0 either way, so assert the rewrite itself: one filter per branch with the
-- pushdown, a single one above the set operation without it. A count() parent collapses the plan,
-- so this probe selects the set-key column instead.
SELECT 'block mismatch filters on', countIf(explain ILIKE '%Filter column: materialize(NULL) = materialize(NULL)%') FROM
(EXPLAIN SELECT DISTINCT x FROM (SELECT DISTINCT NULL AS x INTERSECT ALL SELECT DISTINCT NULL AS x GROUP BY NULL) AS t0 WHERE t0.x = t0.x SETTINGS query_plan_filter_push_down = 1);
SELECT 'block mismatch filters off', countIf(explain ILIKE '%Filter column: materialize(NULL) = materialize(NULL)%') FROM
(EXPLAIN SELECT DISTINCT x FROM (SELECT DISTINCT NULL AS x INTERSECT ALL SELECT DISTINCT NULL AS x GROUP BY NULL) AS t0 WHERE t0.x = t0.x SETTINGS query_plan_filter_push_down = 0);

-- A branch whose GROUP BY keys differ from its sibling's keeps a Const the step materialized away,
-- so that branch input header differs from the output one. Forcing the filter's output header onto
-- it reinstates a constness the branch does not emit, and the plan-time check then fails, so the
-- pushdown must decline. Both settings return 0; the assertion is that the query completes at all.
-- The inner WHERE is required: it is what leaves a second filter to push once the outer one moves.
SELECT 'const branch except', count() FROM (SELECT DISTINCT x FROM ((SELECT DISTINCT NULL AS x GROUP BY 'z', NULL) EXCEPT ALL (SELECT DISTINCT NULL AS x GROUP BY NULL)) WHERE isNullable(-0.)) AS t0 WHERE t0.x > t0.x SETTINGS query_plan_filter_push_down = 1;
SELECT 'const branch except off', count() FROM (SELECT DISTINCT x FROM ((SELECT DISTINCT NULL AS x GROUP BY 'z', NULL) EXCEPT ALL (SELECT DISTINCT NULL AS x GROUP BY NULL)) WHERE isNullable(-0.)) AS t0 WHERE t0.x > t0.x SETTINGS query_plan_filter_push_down = 0;
SELECT 'const branch intersect', count() FROM (SELECT DISTINCT x FROM ((SELECT DISTINCT NULL AS x GROUP BY 'z', NULL) INTERSECT ALL (SELECT DISTINCT NULL AS x GROUP BY NULL)) WHERE isNullable(-0.)) AS t0 WHERE t0.x > t0.x SETTINGS query_plan_filter_push_down = 1;
SELECT 'const branch intersect off', count() FROM (SELECT DISTINCT x FROM ((SELECT DISTINCT NULL AS x GROUP BY 'z', NULL) INTERSECT ALL (SELECT DISTINCT NULL AS x GROUP BY NULL)) WHERE isNullable(-0.)) AS t0 WHERE t0.x > t0.x SETTINGS query_plan_filter_push_down = 0;

-- IntersectOrExceptTransform uniformizes only the main ports, so a WITH TOTALS branch whose main port
-- constant-folds leaves the outer DISTINCT comparing a Const main port against a full totals one.
SELECT 'totals except', count() FROM (SELECT DISTINCT x FROM (SELECT DISTINCT NULL AS x GROUP BY 1, NULL EXCEPT ALL SELECT DISTINCT NULL AS x GROUP BY 'z', NULL WITH TOTALS)) AS t0 WHERE t0.x = t0.x SETTINGS query_plan_filter_push_down = 1;
SELECT 'totals except off', count() FROM (SELECT DISTINCT x FROM (SELECT DISTINCT NULL AS x GROUP BY 1, NULL EXCEPT ALL SELECT DISTINCT NULL AS x GROUP BY 'z', NULL WITH TOTALS)) AS t0 WHERE t0.x = t0.x SETTINGS query_plan_filter_push_down = 0;
SELECT 'totals intersect', count() FROM (SELECT DISTINCT x FROM (SELECT DISTINCT NULL AS x GROUP BY 1, NULL INTERSECT ALL SELECT DISTINCT NULL AS x GROUP BY 'z', NULL WITH TOTALS)) AS t0 WHERE t0.x = t0.x SETTINGS query_plan_filter_push_down = 1;
SELECT 'totals intersect off', count() FROM (SELECT DISTINCT x FROM (SELECT DISTINCT NULL AS x GROUP BY 1, NULL INTERSECT ALL SELECT DISTINCT NULL AS x GROUP BY 'z', NULL WITH TOTALS)) AS t0 WHERE t0.x = t0.x SETTINGS query_plan_filter_push_down = 0;
