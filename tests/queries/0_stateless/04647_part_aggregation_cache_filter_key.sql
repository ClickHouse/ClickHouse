-- Tags: no-parallel

-- Regression test for the part aggregation cache key: the cached per-part state covers only the
-- mark ranges that primary key / skip-index analysis selected for the part, so two queries whose
-- predicates select different ranges of the same part must not share a cache entry. The key is
-- therefore salted with the read step's filter DAG (`ReadFromMergeTree::getFilterActionsDAG`) and
-- with the selected mark ranges of the part.

-- The functional-test config (`tests/config/users.d/limits.yaml`) sets `max_rows_to_group_by = 10G`
-- and read limits (`max_rows_to_read`, `max_bytes_to_read`, `*_leaf`), on which the optimization
-- fails closed; pin them all to 0 so the cache is actually exercised (as in
-- `04033_part_aggregation_cache`).
SET allow_experimental_analyzer = 0, allow_experimental_part_aggregation_cache = 1, optimize_aggregation_in_order = 0, enable_memory_bound_merging_of_aggregation_results = 0, max_rows_to_group_by = 0, max_rows_to_read = 0, max_bytes_to_read = 0, max_rows_to_read_leaf = 0, max_bytes_to_read_leaf = 0;

SYSTEM DROP PART AGGREGATION CACHE;

DROP TABLE IF EXISTS t_part_agg_cache_filter;

-- A small `index_granularity` so that a predicate on the primary key prunes whole marks and the two
-- queries below end up reading different mark ranges of the same part.
CREATE TABLE t_part_agg_cache_filter (k UInt32, g UInt32, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;

-- Keep the part layout stable so the warmed entries stay addressable by part name.
SYSTEM STOP MERGES t_part_agg_cache_filter;

INSERT INTO t_part_agg_cache_filter SELECT number, number % 2, number FROM numbers(32);

-- Exactly one part, so every cache entry below belongs to it.
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_part_agg_cache_filter' AND active;

SELECT count() FROM system.part_aggregation_cache;

-- Query A: `k >= 24` keeps rows 24..31. g=0: 24+26+28+30=108, g=1: 25+27+29+31=112.
SELECT g, sum(v) FROM t_part_agg_cache_filter WHERE k >= 24 GROUP BY g ORDER BY g;

-- One entry for the single part.
SELECT count() FROM system.part_aggregation_cache;

-- The same query again must reuse that entry: the key has to be stable across executions, so no
-- new entry appears and the result does not change.
SELECT g, sum(v) FROM t_part_agg_cache_filter WHERE k >= 24 GROUP BY g ORDER BY g;
SELECT count() FROM system.part_aggregation_cache;

-- Query B differs from A only in the predicate, which selects a wider range of the same part.
-- g=0: 16+18+20+22+24+26+28+30=184, g=1: 17+19+21+23+25+27+29+31=192. A wrong (colliding) key
-- would return query A's cached state here, i.e. 108/112.
SELECT g, sum(v) FROM t_part_agg_cache_filter WHERE k >= 16 GROUP BY g ORDER BY g;

-- Two distinct entries for the same part: one per predicate.
SELECT count() FROM system.part_aggregation_cache;

-- And query A still returns its own result, not query B's.
SELECT g, sum(v) FROM t_part_agg_cache_filter WHERE k >= 24 GROUP BY g ORDER BY g;
SELECT count() FROM system.part_aggregation_cache;

DROP TABLE t_part_agg_cache_filter;
SYSTEM DROP PART AGGREGATION CACHE;
