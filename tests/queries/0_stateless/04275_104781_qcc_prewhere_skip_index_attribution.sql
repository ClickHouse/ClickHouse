-- Tags: no-parallel-replicas, no-parallel
-- Tag no-parallel: messes with the server-level query condition cache

-- Regression test for #104781: a `SELECT ... PREWHERE pk_prefix = X WHERE non_pk IN [...]`
-- against a column with a `bloom_filter` skip index poisons the query condition cache for
-- the PREWHERE predicate. A subsequent benign `SELECT count() ... WHERE pk_prefix = X`
-- then under-counts.
--
-- The bug requires `use_skip_indexes_on_data_read = 1` (default since #93407): the
-- bloom-filter reader runs ahead of PREWHERE and drops marks via `canSkipMark`. Without
-- the fix, those marks are incorrectly attributed to the PREWHERE predicate, even though
-- the predicate matches all rows.
--
-- Settings pinned per-query so that CI randomization cannot disable the bug ingredients:
--   * `use_query_condition_cache = 1`         - the cache that can be poisoned.
--   * `use_skip_indexes_on_data_read = 1`     - puts `MergeTreeReaderIndex` ahead of
--                                               PREWHERE in the reader chain.
--   * `optimize_move_to_prewhere = 1` and
--     `query_plan_optimize_prewhere = 1`      - guarantee `WHERE pk_prefix = X` is hashed
--                                               under the same PREWHERE-predicate key as
--                                               the explicit-PREWHERE trigger query.
--   * `optimize_use_implicit_projections = 0` - disable `_exact_count_projection` so the
--                                               `count()` queries traverse the data path
--                                               and can hit the poisoned cache entry. With
--                                               the implicit projection the count is read
--                                               from per-part metadata and bypasses QCC.

SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS t_104781;

CREATE TABLE t_104781
(
    id String,
    project_id String,
    created_at DateTime64(3) DEFAULT now64(3),
    started_at DateTime64(6),
    INDEX idx_id_bloom id TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(started_at)
ORDER BY (project_id, started_at, id)
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192;

-- 500000 rows in partition 202604, all `project_id = 'P1'`.
INSERT INTO t_104781 (id, project_id, started_at)
SELECT concat('id-', toString(number)), 'P1',
       toDateTime64('2026-04-01 00:00:00', 6) + INTERVAL number SECOND
FROM numbers(500000);

-- 2 rows in partition 202605, same `project_id`, in a separate part.
INSERT INTO t_104781 (id, project_id, started_at)
SELECT concat('id-fresh-', toString(number)), 'P1',
       toDateTime64('2026-05-15 00:00:00', 6) + INTERVAL number SECOND
FROM numbers(2);

SYSTEM DROP QUERY CONDITION CACHE;

SELECT 'truth', count() FROM t_104781 WHERE project_id = 'P1'
SETTINGS use_query_condition_cache = 1, use_skip_indexes_on_data_read = 1,
         optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
         optimize_use_implicit_projections = 0;

-- Trigger: PREWHERE on a primary-key-prefix column + WHERE on a non-PK column with `IN`.
-- The bloom-filter index on `id` drops most marks at read time; before the fix, those
-- marks were attributed to the PREWHERE predicate, poisoning the cache.
SELECT id FROM t_104781
PREWHERE project_id = 'P1'
WHERE id IN ['anything-1', 'anything-2']
FORMAT Null
SETTINGS use_query_condition_cache = 1, use_skip_indexes_on_data_read = 1;

-- With the fix, this returns the full 500002. Before the fix, it returns a smaller, wrong
-- count (the exact under-count is bloom-filter-FPR-dependent).
SELECT 'after_trigger', count() FROM t_104781 WHERE project_id = 'P1'
SETTINGS use_query_condition_cache = 1, use_skip_indexes_on_data_read = 1,
         optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
         optimize_use_implicit_projections = 0;

-- Sanity: cache-off path must match `truth`.
SELECT 'after_trigger_no_cache', count() FROM t_104781 WHERE project_id = 'P1'
SETTINGS use_query_condition_cache = 0, optimize_use_implicit_projections = 0;

-- Sanity: the `use_skip_indexes_on_data_read = 0` workaround (per @nihalzp's bisect)
-- avoids the bug too.
SYSTEM DROP QUERY CONDITION CACHE;
SELECT id FROM t_104781
PREWHERE project_id = 'P1'
WHERE id IN ['anything-1', 'anything-2']
FORMAT Null
SETTINGS use_query_condition_cache = 1, use_skip_indexes_on_data_read = 0;
SELECT 'after_trigger_workaround', count() FROM t_104781 WHERE project_id = 'P1'
SETTINGS use_query_condition_cache = 1, use_skip_indexes_on_data_read = 1,
         optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
         optimize_use_implicit_projections = 0;

-- Sanity: a plain PREWHERE-only query (no bloom-filter-backed `WHERE IN`) still benefits
-- from the PREWHERE-side query condition cache. The fix only suppresses the PREWHERE
-- write when a skip-index reader is in the chain; ordinary cases are unaffected.
DROP TABLE IF EXISTS t_104781_sanity;
CREATE TABLE t_104781_sanity (x UInt32, y UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_104781_sanity SELECT number, number FROM numbers(100000);
SYSTEM DROP QUERY CONDITION CACHE;
SELECT 'sanity_prewhere_1', count() FROM t_104781_sanity PREWHERE x > 50000
SETTINGS use_query_condition_cache = 1, optimize_use_implicit_projections = 0;
SELECT 'sanity_prewhere_2', count() FROM t_104781_sanity PREWHERE x > 50000
SETTINGS use_query_condition_cache = 1, optimize_use_implicit_projections = 0;

DROP TABLE t_104781;
DROP TABLE t_104781_sanity;
