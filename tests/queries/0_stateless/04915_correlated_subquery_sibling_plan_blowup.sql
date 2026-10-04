-- Tags: no-old-analyzer
-- Correlated subqueries are only supported by the analyzer.

SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET enable_parallel_replicas = 0;
SET correlated_subqueries_default_join_kind = 'right';
-- Pinned: a decorrelation join per reference means the default `parallel_hash` builds one hash table
-- per thread per join, which costs tens of gigabytes while the plan is still exponential.
SET join_algorithm = 'hash';

SET correlated_subqueries_use_in_memory_buffer = 0;
-- Unbuffered: one referenced input root serves all eight siblings, so `roots` is 1 and every
-- sibling shows up as a reference to it.
SELECT
    countIf(explain LIKE '%CommonSubplan%' AND explain NOT LIKE '%CommonSubplanReference%') AS roots,
    countIf(explain LIKE '%CommonSubplanReference%') AS refs
FROM (EXPLAIN PLAN optimize = 0 SELECT count() FROM (SELECT number AS x FROM numbers(5)) AS t WHERE ((SELECT max(number) FROM numbers(3) WHERE number < t.x + 0) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 1) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 2) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 3) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 4) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 5) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 6) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 7)) >= 0);

-- The same invariant on the projection path, which reaches a different planner caller
-- (`addExpressionStep`) than the WHERE arm above (`addFilterStep`).
SELECT
    countIf(explain LIKE '%CommonSubplan%' AND explain NOT LIKE '%CommonSubplanReference%') AS roots,
    countIf(explain LIKE '%CommonSubplanReference%') AS refs
FROM (EXPLAIN PLAN optimize = 0 SELECT (SELECT max(number) FROM numbers(3) WHERE number < t.x + 0) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 1) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 2) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 3) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 4) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 5) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 6) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 7) AS s FROM (SELECT number AS x FROM numbers(5)) AS t);

-- And on the `EXISTS` decorrelation kind, which wires its own decorrelation context.
SELECT
    countIf(explain LIKE '%CommonSubplan%' AND explain NOT LIKE '%CommonSubplanReference%') AS roots,
    countIf(explain LIKE '%CommonSubplanReference%') AS refs
FROM (EXPLAIN PLAN optimize = 0 SELECT count() FROM (SELECT number AS x FROM numbers(5)) AS t WHERE exists(SELECT 1 FROM numbers(3) WHERE number < t.x + 0) AND exists(SELECT 1 FROM numbers(3) WHERE number < t.x + 1) AND exists(SELECT 1 FROM numbers(3) WHERE number < t.x + 2) AND exists(SELECT 1 FROM numbers(3) WHERE number < t.x + 3) AND exists(SELECT 1 FROM numbers(3) WHERE number < t.x + 4) AND exists(SELECT 1 FROM numbers(3) WHERE number < t.x + 5) AND exists(SELECT 1 FROM numbers(3) WHERE number < t.x + 6) AND exists(SELECT 1 FROM numbers(3) WHERE number < t.x + 7));

-- Plan size grows linearly in the sibling count, not exponentially: doubling the siblings from four
-- to eight must keep the optimized plan below fourfold (measured 226/118 = 1.9), not the 16.6-fold
-- growth (6895/415) seen when each sibling references its predecessors' subtree. A ratio bound is
-- immune to the randomized join settings that shift both counts together.
SELECT (SELECT count() FROM (EXPLAIN PLAN SELECT count() FROM (SELECT number AS x FROM numbers(5)) AS t WHERE ((SELECT max(number) FROM numbers(3) WHERE number < t.x + 0) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 1) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 2) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 3) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 4) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 5) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 6) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 7)) >= 0))
     < 4 * (SELECT count() FROM (EXPLAIN PLAN SELECT count() FROM (SELECT number AS x FROM numbers(5)) AS t WHERE ((SELECT max(number) FROM numbers(3) WHERE number < t.x + 0) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 1) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 2) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 3)) >= 0));

-- Coarse blow-up backstop: the optimized plan held 6895 lines and now holds at most 242, the
-- randomized settings above moving it over a measured 40-line spread (194 to 242).
SELECT count() < 500 FROM (EXPLAIN PLAN SELECT count() FROM (SELECT number AS x FROM numbers(5)) AS t WHERE ((SELECT max(number) FROM numbers(3) WHERE number < t.x + 0) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 1) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 2) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 3) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 4) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 5) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 6) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 7)) >= 0);

SET correlated_subqueries_use_in_memory_buffer = 1;
-- Buffered: this path is untouched, so it still builds one root per reference (eight of each). The
-- buffer optimization rewrites the referenced step, which a shared root could not survive.
SELECT
    countIf(explain LIKE '%CommonSubplan%' AND explain NOT LIKE '%CommonSubplanReference%') AS roots,
    countIf(explain LIKE '%CommonSubplanReference%') AS refs
FROM (EXPLAIN PLAN optimize = 0 SELECT count() FROM (SELECT number AS x FROM numbers(5)) AS t WHERE ((SELECT max(number) FROM numbers(3) WHERE number < t.x + 0) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 1) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 2) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 3) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 4) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 5) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 6) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 7)) >= 0);

-- Results must not change: each sibling contributes a distinct value and some are NULL.
SELECT 'buf0', t.x, ((SELECT max(number) FROM numbers(2) WHERE number < t.x + 0) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 1) + (SELECT max(number) FROM numbers(4) WHERE number < t.x + 2) + (SELECT max(number) FROM numbers(5) WHERE number < t.x + 3)) AS s
FROM (SELECT number AS x FROM numbers(4)) AS t ORDER BY t.x
SETTINGS correlated_subqueries_use_in_memory_buffer = 0;

SELECT 'buf1', t.x, ((SELECT max(number) FROM numbers(2) WHERE number < t.x + 0) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 1) + (SELECT max(number) FROM numbers(4) WHERE number < t.x + 2) + (SELECT max(number) FROM numbers(5) WHERE number < t.x + 3)) AS s
FROM (SELECT number AS x FROM numbers(4)) AS t ORDER BY t.x
SETTINGS correlated_subqueries_use_in_memory_buffer = 1;

-- The cross-scope shape that reaches the shared-root header guard is rejected identically before and
-- after this change; 04502_correlated_subquery_deep_nested_reference.sql owns it.

-- A nested subquery's own SETTINGS does not reach the query context that decides the plan, so the
-- shared root is not used there; the query still executes normally.
SELECT c FROM (SELECT count() AS c FROM (SELECT number AS x FROM numbers(5)) AS t WHERE ((SELECT max(number) FROM numbers(3) WHERE number < t.x + 0) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 1) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 2) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 3) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 4) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 5) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 6) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 7)) >= 0 SETTINGS correlated_subqueries_use_in_memory_buffer = 0);

-- A `left` join kind disables the buffer on its own, so the shared root is reached here with
-- `correlated_subqueries_use_in_memory_buffer` left at its default. One root still serves all eight
-- siblings, and the swapped join layout must not change the result.
SELECT
    countIf(explain LIKE '%CommonSubplan%' AND explain NOT LIKE '%CommonSubplanReference%') AS roots,
    countIf(explain LIKE '%CommonSubplanReference%') AS refs
FROM (EXPLAIN PLAN optimize = 0 SELECT count() FROM (SELECT number AS x FROM numbers(5)) AS t WHERE ((SELECT max(number) FROM numbers(3) WHERE number < t.x + 0) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 1) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 2) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 3) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 4) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 5) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 6) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 7)) >= 0)
SETTINGS correlated_subqueries_default_join_kind = 'left';

SELECT 'left', t.x, ((SELECT max(number) FROM numbers(2) WHERE number < t.x + 0) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 1) + (SELECT max(number) FROM numbers(4) WHERE number < t.x + 2) + (SELECT max(number) FROM numbers(5) WHERE number < t.x + 3)) AS s
FROM (SELECT number AS x FROM numbers(4)) AS t ORDER BY t.x
SETTINGS correlated_subqueries_default_join_kind = 'left';
