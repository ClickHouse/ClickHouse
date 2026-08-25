
DROP TABLE IF EXISTS test_col_stats_agg_rp;

CREATE TABLE test_col_stats_agg_rp (
    id UInt64,
    value Int32,
    p Int32
) ENGINE = MergeTree
PARTITION BY p
ORDER BY id
SETTINGS auto_statistics_types = 'basic';

SET materialize_statistics_on_insert = 1;

-- p = 1: values {50, 200}; p = 2: values {25, 400}
INSERT INTO test_col_stats_agg_rp VALUES (1, 50, 1), (2, 200, 1);
INSERT INTO test_col_stats_agg_rp VALUES (3, 25, 2), (4, 400, 2);

-- Suppress CI setting randomization to ensure deterministic test behavior.
SET explain_query_plan_default = 'legacy';
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;
SET use_statistics_for_min_max_aggregation = 1;

-- Row-level policies are merged into the same filter the shortcut evaluates on
-- part-level constants, so a policy on a partition key column must still return
-- the filtered extrema (50, 200) through the prepared source.
CREATE ROW POLICY OR REPLACE rp_part ON test_col_stats_agg_rp FOR SELECT USING p = 1 TO ALL;

SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_agg_rp) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value), max(value) FROM test_col_stats_agg_rp;

DROP ROW POLICY rp_part ON test_col_stats_agg_rp;

-- Same contract for a policy on a virtual column.
CREATE ROW POLICY OR REPLACE rp_virt ON test_col_stats_agg_rp FOR SELECT USING _partition_id = '1' TO ALL;

SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_agg_rp) WHERE explain LIKE '%ReadFromPreparedSource%';
SELECT min(value), max(value) FROM test_col_stats_agg_rp;

DROP ROW POLICY rp_virt ON test_col_stats_agg_rp;

-- A policy that needs row data cannot be enforced by the shortcut: the query
-- must fall back to ReadFromMergeTree.
CREATE ROW POLICY OR REPLACE rp_row ON test_col_stats_agg_rp FOR SELECT USING value > 100 TO ALL;

SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_agg_rp) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT min(value), max(value) FROM test_col_stats_agg_rp;

-- Even with a part-level-resolvable user WHERE on top, the non-partition
-- policy conjunct keeps the whole filter off the shortcut.
SELECT trimLeft(explain) FROM (EXPLAIN SELECT min(value), max(value) FROM test_col_stats_agg_rp WHERE p = 1) WHERE explain LIKE '%ReadFromMergeTree%';
SELECT min(value), max(value) FROM test_col_stats_agg_rp WHERE p = 1;

DROP ROW POLICY rp_row ON test_col_stats_agg_rp;

DROP TABLE test_col_stats_agg_rp;
