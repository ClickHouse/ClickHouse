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
-- One referenced input root serves all eight siblings. Eight roots here means each sibling references
-- the plan grown by its predecessors, which is what made the optimized plan double per sibling.
SELECT
    countIf(explain LIKE '%CommonSubplan%' AND explain NOT LIKE '%CommonSubplanReference%') AS roots,
    countIf(explain LIKE '%CommonSubplanReference%') AS refs
FROM (EXPLAIN PLAN optimize = 0 SELECT count() FROM (SELECT number AS x FROM numbers(5)) AS t WHERE ((SELECT max(number) FROM numbers(3) WHERE number < t.x + 0) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 1) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 2) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 3) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 4) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 5) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 6) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 7)) >= 0);

-- The optimized plan held 6895 lines and now holds at most 242; randomized join settings move that
-- by up to 48, so the bound is well clear of both.
SELECT count() < 500 FROM (EXPLAIN PLAN SELECT count() FROM (SELECT number AS x FROM numbers(5)) AS t WHERE ((SELECT max(number) FROM numbers(3) WHERE number < t.x + 0) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 1) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 2) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 3) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 4) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 5) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 6) + (SELECT max(number) FROM numbers(3) WHERE number < t.x + 7)) >= 0);

SET correlated_subqueries_use_in_memory_buffer = 1;
-- The buffered path is untouched: it still builds one root per reference.
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
