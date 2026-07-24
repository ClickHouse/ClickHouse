-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: messes with the (instance-wide) query condition cache
-- Tag no-parallel-replicas: this test drives parallel replicas explicitly; the query condition cache
--   is populated per replica, so the poisoning is deterministic only with a fixed parallel-replicas setup

-- Regression test for the query condition cache being poisoned by a parallel-replicas read of a
-- Merge table (or merge() table function) over a VIEW (issue #111363).
--
-- The view predicate goes into the ReadFromMergeTree step (as PREWHERE), while the OUTER merge() query
-- predicate stays as a separate FilterStep on a different column. Under parallel replicas with a local
-- plan the two do not fuse. The cache write attributed the outer FilterStep's all-false verdict to the
-- VIEW predicate's hash, so a later plain query using the view predicate alone wrongly skipped every
-- mark and returned 0 rows.
--
-- The poisoning only manifests when the view predicate reaches the read step as PREWHERE and the
-- initiator runs a local plan, so those settings are pinned explicitly (they would otherwise be
-- randomized and make the test flaky). use_query_condition_cache is pinned per-query for the same reason.

DROP TABLE IF EXISTS m_base;
DROP TABLE IF EXISTS m_view;

CREATE TABLE m_base (v Int64, s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO m_base SELECT number, toString(number % 7) FROM numbers(100);
CREATE VIEW m_view AS SELECT * FROM m_base WHERE v != 42;

SYSTEM DROP QUERY CONDITION CACHE;

-- The poisoning query: a parallel-replicas read of merge() over the view whose outer predicate matches
-- no rows. Returns 0 (correct) but used to write an all-false cache entry keyed on the view's predicate.
SELECT count() FROM merge('^m_view$') WHERE s = 'nope'
SETTINGS use_query_condition_cache = 1,
         allow_experimental_parallel_reading_from_replicas = 1,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1,
         max_parallel_replicas = 3,
         parallel_replicas_local_plan = 1,
         automatic_parallel_replicas_mode = 0,
         optimize_move_to_prewhere = 1,
         query_plan_optimize_prewhere = 1;

-- A later plain query on the base table using the same predicate must return all matching rows (99),
-- not 0. Before the fix this returned 0 because the poisoned cache entry skipped the mark.
SELECT count() FROM m_base WHERE v != 42 SETTINGS use_query_condition_cache = 1;

-- The same result must hold with the cache disabled (ground truth).
SELECT count() FROM m_base WHERE v != 42 SETTINGS use_query_condition_cache = 0;

DROP TABLE m_view;

-- Same scenario with an IN() view predicate: the read condition (and cache key) is a `v IN (...)`
-- atom rather than a scalar comparison. This exercises the atom comparator on a set predicate and
-- confirms the fix is not specific to the != shape.
DROP VIEW IF EXISTS m_view_in;
CREATE VIEW m_view_in AS SELECT * FROM m_base WHERE v IN (1, 2, 3);

SYSTEM DROP QUERY CONDITION CACHE;

SELECT count() FROM merge('^m_view_in$') WHERE s = 'nope'
SETTINGS use_query_condition_cache = 1,
         allow_experimental_parallel_reading_from_replicas = 1,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1,
         max_parallel_replicas = 3,
         parallel_replicas_local_plan = 1,
         automatic_parallel_replicas_mode = 0,
         optimize_move_to_prewhere = 1,
         query_plan_optimize_prewhere = 1;

-- Must return the 3 matching rows, not 0.
SELECT count() FROM m_base WHERE v IN (1, 2, 3) SETTINGS use_query_condition_cache = 1;
SELECT count() FROM m_base WHERE v IN (1, 2, 3) SETTINGS use_query_condition_cache = 0;

DROP VIEW m_view_in;
DROP TABLE m_base;

-- Qualifier-collision edge case: a table with BOTH a column `k` and a real column literally named
-- `__table1.k` (which the storage domain renders as the bare identifier `__table1.k`). The comparator
-- strips a leading `__tableN.` qualifier, so the outer filter on `k` and the view read condition on
-- `__table1.k` both reduce to `k`. This confirms the outer predicate is still not attributed to the
-- view predicate's hash across that name collision, so a later plain query on `__table1.k` is not
-- poisoned. (The read predicate and the outer predicate stay structurally distinct, so the atoms do
-- not match even though their stripped leaf names coincide.)
DROP TABLE IF EXISTS c_base;
DROP VIEW IF EXISTS c_view;
CREATE TABLE c_base (k Int64, `__table1.k` Int64, s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO c_base SELECT number, number + 1000, toString(number % 7) FROM numbers(100);
CREATE VIEW c_view AS SELECT * FROM c_base WHERE `__table1.k` = 1042;

SYSTEM DROP QUERY CONDITION CACHE;

-- Poison attempt: outer filter on `k` = 1042 matches no rows (k in [0, 99]); its stripped leaf name
-- collides with the view predicate's stripped leaf name.
SELECT count() FROM merge('^c_view$') WHERE k = 1042
SETTINGS use_query_condition_cache = 1,
         allow_experimental_parallel_reading_from_replicas = 1,
         cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
         parallel_replicas_for_non_replicated_merge_tree = 1,
         max_parallel_replicas = 3,
         parallel_replicas_local_plan = 1,
         automatic_parallel_replicas_mode = 0,
         optimize_move_to_prewhere = 1,
         query_plan_optimize_prewhere = 1;

-- Later plain query on the real `__table1.k` column must return its 1 matching row, not 0.
SELECT count() FROM c_base WHERE `__table1.k` = 1042 SETTINGS use_query_condition_cache = 1;
SELECT count() FROM c_base WHERE `__table1.k` = 1042 SETTINGS use_query_condition_cache = 0;

DROP VIEW c_view;
DROP TABLE c_base;
