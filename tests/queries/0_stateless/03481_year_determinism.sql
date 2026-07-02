-- Tags: no-parallel, no-shared-merge-tree
-- Tag no-parallel: uses the query result cache, which is a global singleton.
-- Tag no-shared-merge-tree: SharedMergeTree allows non-deterministic mutations, so the
--   Replicated-rejection assertions below do not hold there.

-- Only the niladic year() form is non-deterministic (it reads the wall clock, like now()/today()).
-- year(<date>) delegates to toYear and must stay deterministic, so it keeps working with the query
-- result cache and in Replicated mutations, exactly like toYear(<date>).

SELECT '-- query result cache: year(<date>) is cacheable like toYear(<date>)';
SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS 03481_qc;
CREATE TABLE 03481_qc (ts DateTime) ENGINE = MergeTree ORDER BY ts;
INSERT INTO 03481_qc VALUES ('2024-06-01 00:00:00'), ('2023-01-01 00:00:00');

-- toYear(<date>) caches (baseline), and year(<date>) must behave identically. The query-cache
-- determinism check runs on the initiator, so pin enable_parallel_replicas = 0 to keep the test
-- focused on determinism (it is orthogonal to distributed routing).
SELECT count() FROM 03481_qc WHERE toYear(ts) = 2024 SETTINGS use_query_cache = 1, enable_parallel_replicas = 0;
SELECT count() FROM 03481_qc WHERE year(ts) = 2024 SETTINGS use_query_cache = 1, enable_parallel_replicas = 0;
SELECT '-- both are stored in the query cache (one entry each)';
SELECT count() FROM system.query_cache WHERE query LIKE '%03481_qc%';

SYSTEM DROP QUERY CACHE;
SELECT '-- niladic year() is non-deterministic and is rejected from the query cache';
SELECT year() SETTINGS use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT year() SETTINGS use_query_cache = 1, query_cache_nondeterministic_function_handling = 'throw'; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT '-- nothing was cached for niladic year()';
SELECT count() FROM system.query_cache;

DROP TABLE 03481_qc;
SYSTEM DROP QUERY CACHE;

SELECT '-- Replicated mutations: ALTER ... WHERE year(<date>) is accepted like toYear(<date>)';
DROP TABLE IF EXISTS 03481_rep SYNC;
CREATE TABLE 03481_rep (ts DateTime, v UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/03481_year_determinism', '1')
ORDER BY ts;
INSERT INTO 03481_rep VALUES ('2024-06-01 00:00:00', 1), ('2023-01-01 00:00:00', 2), ('2022-01-01 00:00:00', 3);

-- Deterministic predicates are accepted without allow_nondeterministic_mutations. The point of the
-- test is that the pre-build determinism check does not reject year(<date>), so assert acceptance
-- via system.mutations (matching toYear) rather than racing on the mutated data.
ALTER TABLE 03481_rep DELETE WHERE year(ts) = 2022 SETTINGS mutations_sync = 2;
ALTER TABLE 03481_rep DELETE WHERE toYear(ts) = 2024 SETTINGS mutations_sync = 2;
SELECT '-- both year(<date>) and toYear(<date>) DELETE mutations were accepted and completed';
SELECT command, is_done FROM system.mutations
WHERE database = currentDatabase() AND table = '03481_rep'
ORDER BY command;

SELECT '-- niladic year() stays rejected in a Replicated mutation';
ALTER TABLE 03481_rep DELETE WHERE ts < makeDateTime(year(), 1, 1, 0, 0, 0) SETTINGS mutations_sync = 2; -- { serverError BAD_ARGUMENTS }

DROP TABLE 03481_rep SYNC;

SELECT '-- projections: year(<date>) filters/keys still select projections like toYear(<date>)';
-- The projection implication checks run on the post-build ActionsDAG node (function_base), which for
-- year(<date>) is toYear's deterministic base via the resolver's build() delegation. So a normal or
-- aggregate projection defined over year(...) is still used for year(...) queries, exactly like toYear.
-- Projection selection is orthogonal to parallel-replica routing, so pin enable_parallel_replicas = 0
-- to keep the test focused (the standalone test server has no parallel_replicas cluster).
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
