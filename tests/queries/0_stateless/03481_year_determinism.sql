-- Tags: no-parallel, no-shared-merge-tree
-- Tag no-parallel: uses the query result cache, which is a global singleton.
-- Tag no-shared-merge-tree: SharedMergeTree allows non-deterministic mutations, so the
--   Replicated-rejection assertions below do not hold there.

-- The year() function is non-deterministic (its niladic form reads the wall clock, like
-- now()/today()). The pre-build determinism checks inspect the unresolved AST by function name and
-- cannot tell year() from year(<date>), so the whole function is treated as non-deterministic:
-- year(<date>) is excluded from the query result cache and rejected in Replicated mutations. Use
-- toYear(<date>) where determinism is required. Index/projection analysis, however, works on the
-- resolved (post-build) function and is unaffected, so year(<date>) still prunes granules and uses
-- projections exactly like toYear(<date>).

SELECT '-- query result cache: year(<date>) is non-deterministic (not cached), unlike toYear(<date>)';
SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS 03481_qc;
CREATE TABLE 03481_qc (ts DateTime) ENGINE = MergeTree ORDER BY ts;
INSERT INTO 03481_qc VALUES ('2024-06-01 00:00:00'), ('2023-01-01 00:00:00');

-- The query-cache determinism check runs on the initiator, so pin enable_parallel_replicas = 0 to
-- keep the test focused on determinism (it is orthogonal to distributed routing).
SELECT '-- toYear(<date>) is deterministic and caches';
SELECT count() FROM 03481_qc WHERE toYear(ts) = 2024 SETTINGS use_query_cache = 1, query_cache_nondeterministic_function_handling = 'throw', enable_parallel_replicas = 0;
SELECT '-- year(<date>) is rejected from the query cache';
SELECT count() FROM 03481_qc WHERE year(ts) = 2024 SETTINGS use_query_cache = 1, query_cache_nondeterministic_function_handling = 'throw', enable_parallel_replicas = 0; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT '-- niladic year() is rejected from the query cache too';
SELECT year() SETTINGS use_query_cache = 1, query_cache_nondeterministic_function_handling = 'throw'; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT '-- only the deterministic toYear(<date>) query is cached';
SELECT count() FROM system.query_cache WHERE query LIKE '%03481_qc%';

DROP TABLE 03481_qc;
SYSTEM DROP QUERY CACHE;

SELECT '-- Replicated mutations: year(<date>) is rejected (non-deterministic), toYear(<date>) is accepted';
DROP TABLE IF EXISTS 03481_rep SYNC;
CREATE TABLE 03481_rep (ts DateTime, v UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03481_year_determinism', '1')
ORDER BY ts;
INSERT INTO 03481_rep VALUES ('2024-06-01 00:00:00', 1), ('2023-01-01 00:00:00', 2), ('2022-01-01 00:00:00', 3);

ALTER TABLE 03481_rep DELETE WHERE toYear(ts) = 2024 SETTINGS mutations_sync = 2;
SELECT '-- the toYear(<date>) DELETE mutation was accepted and completed';
SELECT command, is_done FROM system.mutations
WHERE database = currentDatabase() AND table = '03481_rep'
ORDER BY command;
SELECT '-- year(<date>) mutation is rejected as non-deterministic';
ALTER TABLE 03481_rep DELETE WHERE year(ts) = 2022 SETTINGS mutations_sync = 2; -- { serverError BAD_ARGUMENTS }

DROP TABLE 03481_rep SYNC;

SELECT '-- index analysis: year(<date>) still prunes granules like toYear(<date>) (post-build analysis is unaffected by determinism)';
DROP TABLE IF EXISTS 03481_idx;
CREATE TABLE 03481_idx (d Date) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 8192;
INSERT INTO 03481_idx SELECT toDate('2000-01-01') + number FROM numbers(20000);
SELECT '-- both toYear(d) and year(d) prune to the same 1/2 granule count';
-- Pin enable_parallel_replicas = 0 so the EXPLAIN reflects local granule pruning (parallel-replica
-- routing is orthogonal to index analysis and would add distributed plan steps). Match the exact
-- pruning fraction 'Granules: 1/2' (not a bare 'Granules:'), so the read-summary line some
-- randomized settings add ('Parts: 1 | Granules: 1', no slash) does not leak into the output.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM 03481_idx WHERE toYear(d) = 2005 SETTINGS enable_parallel_replicas = 0) WHERE explain ILIKE '%Granules: 1/2%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM 03481_idx WHERE year(d) = 2005 SETTINGS enable_parallel_replicas = 0) WHERE explain ILIKE '%Granules: 1/2%';
DROP TABLE 03481_idx;

SELECT '-- projections: year(<date>) filters/keys still select projections like toYear(<date>)';
-- The projection implication checks run on the post-build ActionsDAG node (function_base), which for
-- year(<date>) is toYear's base via the resolver's build() delegation, so a projection defined over
-- year(...) is still used for year(...) queries, exactly like toYear. Projection selection is
-- orthogonal to parallel-replica routing, so pin enable_parallel_replicas = 0 to keep the test
-- focused (the standalone test server has no parallel_replicas cluster).
SET enable_parallel_replicas = 0;
DROP TABLE IF EXISTS 03481_proj;
CREATE TABLE 03481_proj
(
    id UInt32,
    ts DateTime,
    val UInt32,
    PROJECTION p_by_year (SELECT id, ts, val ORDER BY year(ts))
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1000;
INSERT INTO 03481_proj SELECT number, toDateTime('2015-01-01 00:00:00') + number * 43200, number FROM numbers(50000);

SELECT '-- normal projection is chosen for both toYear(ts) and year(ts) filters';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT id, ts, val FROM 03481_proj WHERE toYear(ts) = 2018 SETTINGS optimize_use_projections = 1, force_optimize_projection = 0) WHERE explain ILIKE '%p_by_year%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT id, ts, val FROM 03481_proj WHERE year(ts) = 2018 SETTINGS optimize_use_projections = 1, force_optimize_projection = 0) WHERE explain ILIKE '%p_by_year%';
SELECT '-- force_optimize_projection = 1 succeeds for year(ts) (would throw PROJECTION_NOT_USED if rejected), same result as toYear(ts)';
SELECT count() FROM 03481_proj WHERE toYear(ts) = 2018 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT count() FROM 03481_proj WHERE year(ts) = 2018 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE 03481_proj;

DROP TABLE IF EXISTS 03481_agg;
CREATE TABLE 03481_agg
(
    ts DateTime,
    v UInt32,
    PROJECTION p_agg_year (SELECT year(ts) AS y, sum(v) GROUP BY y)
)
ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 8192;
INSERT INTO 03481_agg SELECT toDateTime('2019-01-01 00:00:00') + number * 86400, number FROM numbers(3000);

SELECT '-- aggregate projection is chosen for a year(ts) GROUP BY, and force_optimize_projection = 1 succeeds';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT year(ts) AS y, sum(v) FROM 03481_agg GROUP BY y SETTINGS optimize_use_projections = 1, force_optimize_projection = 0) WHERE explain ILIKE '%p_agg_year%';
SELECT count() FROM (SELECT year(ts) AS y, sum(v) FROM 03481_agg GROUP BY y SETTINGS optimize_use_projections = 1, force_optimize_projection = 1);

DROP TABLE 03481_agg;
