-- https://github.com/ClickHouse/ClickHouse/issues/110814
-- A single-table (residual, non-equi) condition in the JOIN ON clause must be
-- applied identically regardless of join_algorithm.

-- The single-table ON-filter handling lives only in the analyzer's logical join
-- step; pin the analyzer so the coverage job that defaults to the legacy
-- interpreter still runs the intended path.
SET enable_analyzer = 1;

DROP TABLE IF EXISTS jl;
DROP TABLE IF EXISTS jr;
CREATE TABLE jl (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE jr (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO jl VALUES (1), (2), (3);
INSERT INTO jr VALUES (1), (2), (3);

-- Equi-key plus a single-table filter: every algorithm returns the same rows.
SELECT 'hash', groupArray(id) FROM (SELECT l.id AS id FROM jl AS l JOIN jr AS r ON l.id = r.k AND l.id > 1 ORDER BY l.id SETTINGS join_algorithm = 'hash');
SELECT 'parallel_hash', groupArray(id) FROM (SELECT l.id AS id FROM jl AS l JOIN jr AS r ON l.id = r.k AND l.id > 1 ORDER BY l.id SETTINGS join_algorithm = 'parallel_hash');
SELECT 'partial_merge', groupArray(id) FROM (SELECT l.id AS id FROM jl AS l JOIN jr AS r ON l.id = r.k AND l.id > 1 ORDER BY l.id SETTINGS join_algorithm = 'partial_merge');
SELECT 'full_sorting_merge', groupArray(id) FROM (SELECT l.id AS id FROM jl AS l JOIN jr AS r ON l.id = r.k AND l.id > 1 ORDER BY l.id SETTINGS join_algorithm = 'full_sorting_merge');
SELECT 'grace_hash', groupArray(id) FROM (SELECT l.id AS id FROM jl AS l JOIN jr AS r ON l.id = r.k AND l.id > 1 ORDER BY l.id SETTINGS join_algorithm = 'grace_hash');
SELECT 'auto', groupArray(id) FROM (SELECT l.id AS id FROM jl AS l JOIN jr AS r ON l.id = r.k AND l.id > 1 ORDER BY l.id SETTINGS join_algorithm = 'auto');
SELECT 'prefer_partial_merge', groupArray(id) FROM (SELECT l.id AS id FROM jl AS l JOIN jr AS r ON l.id = r.k AND l.id > 1 ORDER BY l.id SETTINGS join_algorithm = 'prefer_partial_merge');

-- Keyless single-table condition: an INNER join with no join keys is a CROSS join,
-- so it applies the filter identically for every algorithm.
SELECT 'keyless_hash', groupArray(id) FROM (SELECT l.id AS id FROM jl AS l JOIN jr AS r ON l.id > 1 ORDER BY l.id SETTINGS join_algorithm = 'hash');
SELECT 'keyless_parallel_hash', groupArray(id) FROM (SELECT l.id AS id FROM jl AS l JOIN jr AS r ON l.id > 1 ORDER BY l.id SETTINGS join_algorithm = 'parallel_hash');
SELECT 'keyless_partial_merge', groupArray(id) FROM (SELECT l.id AS id FROM jl AS l JOIN jr AS r ON l.id > 1 ORDER BY l.id SETTINGS join_algorithm = 'partial_merge');
SELECT 'keyless_full_sorting_merge', groupArray(id) FROM (SELECT l.id AS id FROM jl AS l JOIN jr AS r ON l.id > 1 ORDER BY l.id SETTINGS join_algorithm = 'full_sorting_merge');
SELECT 'keyless_grace_hash', groupArray(id) FROM (SELECT l.id AS id FROM jl AS l JOIN jr AS r ON l.id > 1 ORDER BY l.id SETTINGS join_algorithm = 'grace_hash');
SELECT 'keyless_default', groupArray(id) FROM (SELECT l.id AS id FROM jl AS l JOIN jr AS r ON l.id > 1 ORDER BY l.id SETTINGS join_algorithm = 'default');
SELECT 'keyless_auto', groupArray(id) FROM (SELECT l.id AS id FROM jl AS l JOIN jr AS r ON l.id > 1 ORDER BY l.id SETTINGS join_algorithm = 'auto');
SELECT 'keyless_prefer_partial_merge', groupArray(id) FROM (SELECT l.id AS id FROM jl AS l JOIN jr AS r ON l.id > 1 ORDER BY l.id SETTINGS join_algorithm = 'prefer_partial_merge');
SELECT 'keyless_direct', groupArray(id) FROM (SELECT l.id AS id FROM jl AS l JOIN jr AS r ON l.id > 1 ORDER BY l.id SETTINGS join_algorithm = 'direct');

-- A two-table non-equi INNER condition also yields no equi-key, so it is a CROSS
-- join and must return the same rows for every algorithm.
SELECT 'nonequi_hash', groupArray((lid, rk)) FROM (SELECT l.id AS lid, r.k AS rk FROM jl AS l JOIN jr AS r ON l.id > r.k ORDER BY lid, rk SETTINGS join_algorithm = 'hash');
SELECT 'nonequi_full_sorting_merge', groupArray((lid, rk)) FROM (SELECT l.id AS lid, r.k AS rk FROM jl AS l JOIN jr AS r ON l.id > r.k ORDER BY lid, rk SETTINGS join_algorithm = 'full_sorting_merge');
SELECT 'nonequi_grace_hash', groupArray((lid, rk)) FROM (SELECT l.id AS lid, r.k AS rk FROM jl AS l JOIN jr AS r ON l.id > r.k ORDER BY lid, rk SETTINGS join_algorithm = 'grace_hash');

-- A predicate on the preserved side of an outer join (the left side of LEFT JOIN,
-- the right side of RIGHT JOIN) cannot be pushed down and stays rejected for every
-- algorithm.
SELECT l.id FROM jl AS l LEFT JOIN jr AS r ON l.id > 1 SETTINGS join_algorithm = 'hash'; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT l.id FROM jl AS l LEFT JOIN jr AS r ON l.id > 1 SETTINGS join_algorithm = 'full_sorting_merge'; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT r.k FROM jl AS l RIGHT JOIN jr AS r ON r.k > 1 SETTINGS join_algorithm = 'hash'; -- { serverError INVALID_JOIN_ON_EXPRESSION }
SELECT r.k FROM jl AS l RIGHT JOIN jr AS r ON r.k > 1 SETTINGS join_algorithm = 'full_sorting_merge'; -- { serverError INVALID_JOIN_ON_EXPRESSION }

-- A predicate on the non-preserved (inner) side is pushed down as a pre-join filter
-- and must give the same result for every algorithm.
SELECT 'left_inner_side', groupArray((lid, rk)) FROM (SELECT l.id AS lid, r.k AS rk FROM jl AS l LEFT JOIN jr AS r ON r.k > 1 ORDER BY lid, rk SETTINGS join_algorithm = 'hash');
SELECT 'left_inner_side', groupArray((lid, rk)) FROM (SELECT l.id AS lid, r.k AS rk FROM jl AS l LEFT JOIN jr AS r ON r.k > 1 ORDER BY lid, rk SETTINGS join_algorithm = 'full_sorting_merge');
SELECT 'right_inner_side', groupArray((lid, rk)) FROM (SELECT l.id AS lid, r.k AS rk FROM jl AS l RIGHT JOIN jr AS r ON l.id > 1 ORDER BY lid, rk SETTINGS join_algorithm = 'hash');
SELECT 'right_inner_side', groupArray((lid, rk)) FROM (SELECT l.id AS lid, r.k AS rk FROM jl AS l RIGHT JOIN jr AS r ON l.id > 1 ORDER BY lid, rk SETTINGS join_algorithm = 'full_sorting_merge');

-- Liveness: a keyed join keeps its selected merge algorithm, while keyless and
-- constant joins are forced onto HashJoin. Result-only assertions cannot tell
-- these apart, and each of the keyless and constant fallbacks is a distinct branch.
SELECT 'keyed_keeps_merge', count() > 0 FROM (EXPLAIN actions = 1 SELECT l.id FROM jl AS l JOIN jr AS r ON l.id = r.k AND l.id > 1 SETTINGS join_algorithm = 'full_sorting_merge') WHERE explain ILIKE '%Algorithm: FullSortingMergeJoin%';
SELECT 'keyless_not_merge', countIf(explain ILIKE '%Algorithm: HashJoin%') > 0 AND countIf(explain ILIKE '%FullSortingMergeJoin%') = 0 FROM (EXPLAIN actions = 1 SELECT l.id FROM jl AS l JOIN jr AS r ON l.id > 1 SETTINGS join_algorithm = 'full_sorting_merge', query_plan_enable_optimizations = 0);
SELECT 'const_not_merge', countIf(explain ILIKE '%Algorithm: HashJoin%') > 0 AND countIf(explain ILIKE '%FullSortingMergeJoin%') = 0 FROM (EXPLAIN actions = 1 SELECT l.id FROM jl AS l JOIN jr AS r ON 1 = 1 SETTINGS join_algorithm = 'full_sorting_merge', query_plan_optimize_join_order_limit = 0);

DROP TABLE jl;
DROP TABLE jr;
