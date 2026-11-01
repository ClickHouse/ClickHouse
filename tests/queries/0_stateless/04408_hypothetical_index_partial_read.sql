-- Tags: no-replicated-database, no-random-merge-tree-settings
-- no-replicated-database: hypothetical indexes are session-scoped and not replicated
-- no-random-merge-tree-settings: needs a deterministic granule layout

DROP TABLE IF EXISTS t_hypo_partial;
CREATE TABLE t_hypo_partial (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 100, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- 100 granules of 100 rows, a >= 9800 prunes the baseline to the last 3 granules (300 rows)
INSERT INTO t_hypo_partial SELECT number, number FROM numbers(10000);

CREATE HYPOTHETICAL INDEX idx_b ON t_hypo_partial (b) TYPE minmax GRANULARITY 1;

-- empirical EXPLAIN WHATIF reads only the baseline windows, not the whole part
-- with max_rows_to_read = 1000 a whole-part scan (10000 rows) would raise TOO_MANY_ROWS so it should survive
SELECT replaceRegexpAll(trim(explain), ' +', ' ') AS line
FROM (
    EXPLAIN WHATIF SELECT * FROM t_hypo_partial WHERE a >= 9800 AND b = 9850
    SETTINGS max_rows_to_read = 1000, merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0
)
WHERE explain LIKE '%skip_ratio:%' OR explain LIKE '%source:%'
ORDER BY line;

DROP TABLE IF EXISTS t_hypo_partial;
