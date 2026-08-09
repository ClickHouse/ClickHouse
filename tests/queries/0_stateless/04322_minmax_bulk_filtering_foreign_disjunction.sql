-- Tags: no-parallel-replicas
-- no-parallel-replicas: per-query SETTINGS toggling skip-index evaluation paths
-- must take effect on the executing replica.

-- The minmax bulk-filtering path (use_minmax_index_bulk_filtering) does not populate the
-- partial-disjunction bitset, so it can only run alongside use_skip_indexes_for_disjunctions
-- when no disjunction crosses a leaf the minmax index owns. This test pins the evaluation
-- path and checks, via the IndexBulkFilteringEvaluatedGranules profile event, that:
--   * a foreign disjunction (OR over a different indexed column) still lets the index whose
--     own leaf stays outside the OR take the bulk path, and
--   * a disjunction that crosses the indexed columns correctly falls back to the scalar path,
-- in addition to verifying answer parity in both cases.

SET secondary_indices_enable_bulk_filtering = 1;
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS t_minmax_disj;

CREATE TABLE t_minmax_disj
(
    t Int64,
    v Int64,
    INDEX idx_t t TYPE minmax GRANULARITY 1,
    INDEX idx_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 128;

INSERT INTO t_minmax_disj SELECT number AS t, number * 7 % 10000 AS v FROM numbers(20000);

-- Answer parity: bulk on vs off must agree for both shapes, with disjunctions enabled.

SELECT 'foreign disjunction parity',
    (SELECT count() FROM t_minmax_disj WHERE t >= 5000 AND (v < 100 OR v > 9900)
         SETTINGS use_minmax_index_bulk_filtering = 0, use_skip_indexes_for_disjunctions = 1) =
    (SELECT count() FROM t_minmax_disj WHERE t >= 5000 AND (v < 100 OR v > 9900)
         SETTINGS use_minmax_index_bulk_filtering = 1, use_skip_indexes_for_disjunctions = 1) AS ok;

SELECT 'crossing disjunction parity',
    (SELECT count() FROM t_minmax_disj WHERE t < 2000 OR v > 9900
         SETTINGS use_minmax_index_bulk_filtering = 0, use_skip_indexes_for_disjunctions = 1) =
    (SELECT count() FROM t_minmax_disj WHERE t < 2000 OR v > 9900
         SETTINGS use_minmax_index_bulk_filtering = 1, use_skip_indexes_for_disjunctions = 1) AS ok;

-- A rejected mark between two survivors must not be reintroduced by seek-gap merging before
-- another index writes the foreign disjunction's bits. Bulk does not write idx_t's owned-leaf
-- bits, so carrying the middle mark forward would make its default `true` look like `t = 1`.
DROP TABLE IF EXISTS t_minmax_disj_gap;

CREATE TABLE t_minmax_disj_gap
(
    t UInt8,
    v UInt8,
    INDEX idx_gap_t t TYPE minmax GRANULARITY 1,
    INDEX idx_gap_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t_minmax_disj_gap VALUES (1, 0), (0, 1), (1, 0);

SELECT 'foreign disjunction seek-gap pruning',
    (SELECT count() FROM t_minmax_disj_gap WHERE t = 1 AND (v = 1 OR v = 2)
         SETTINGS use_minmax_index_bulk_filtering = 0, use_skip_indexes_for_disjunctions = 1,
                  merge_tree_min_rows_for_seek = 100) =
    (SELECT count() FROM t_minmax_disj_gap WHERE t = 1 AND (v = 1 OR v = 2)
         SETTINGS use_minmax_index_bulk_filtering = 1, use_skip_indexes_for_disjunctions = 1,
                  merge_tree_min_rows_for_seek = 100) AS parity,
    (SELECT count() > 0
     FROM
     (
         EXPLAIN indexes = 1
         SELECT * FROM t_minmax_disj_gap WHERE t = 1 AND (v = 1 OR v = 2)
         SETTINGS use_minmax_index_bulk_filtering = 1, use_skip_indexes_for_disjunctions = 1,
                  merge_tree_min_rows_for_seek = 100, parallel_replicas_local_plan = 1
     )
     WHERE explain ILIKE '%Ranges: 0%') AS pruned;

DROP TABLE t_minmax_disj_gap;

-- Path selection: the foreign disjunction must keep idx_t on the bulk path (its t >= 5000 leaf
-- stays outside the v-only OR); the crossing disjunction must put both indexes on the scalar
-- path (each index owns a leaf inside the OR), so no granule is evaluated by bulk.

SELECT count() FROM t_minmax_disj WHERE t >= 5000 AND (v < 100 OR v > 9900)
    SETTINGS use_minmax_index_bulk_filtering = 1, use_skip_indexes_for_disjunctions = 1,
             log_comment = '04322_foreign_disj', compile_expressions = 0
FORMAT Null;

SELECT count() FROM t_minmax_disj WHERE t < 2000 OR v > 9900
    SETTINGS use_minmax_index_bulk_filtering = 1, use_skip_indexes_for_disjunctions = 1,
             log_comment = '04322_crossing_disj', compile_expressions = 0
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'foreign disjunction uses bulk',
    ProfileEvents['IndexBulkFilteringEvaluatedGranules'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04322_foreign_disj'
  AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT 'crossing disjunction skips bulk',
    ProfileEvents['IndexBulkFilteringEvaluatedGranules'] = 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04322_crossing_disj'
  AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_minmax_disj;
