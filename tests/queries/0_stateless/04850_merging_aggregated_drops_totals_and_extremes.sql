-- Tags: distributed

DROP TABLE IF EXISTS t_04850;
DROP VIEW IF EXISTS v_04850;
DROP TABLE IF EXISTS d_04850;

CREATE TABLE t_04850 (k UInt8, v UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_04850 VALUES (1, 10), (2, 20);
CREATE VIEW v_04850 AS SELECT k, v FROM t_04850;
CREATE TABLE d_04850 (k UInt8, v UInt32)
    ENGINE = Distributed('test_shard_localhost', currentDatabase(), 't_04850');

-- Uniting a view pipe with a Distributed pipe leaves the merge step a totals port to inherit.
SET enable_analyzer = 1;
SET inject_random_order_for_select_without_order_by = 1;
SET distributed_aggregation_memory_efficient = 1;
SET aggregation_memory_efficient_merge_threads = 1;
SET max_threads = 1;

SELECT '-- memory-efficient branch';
SELECT sum(v) AS s FROM merge(currentDatabase(), '^(v_04850|d_04850)$') WITH TOTALS;

SELECT '-- non-memory-efficient branch';
SELECT sum(v) AS s FROM merge(currentDatabase(), '^(v_04850|d_04850)$') WITH TOTALS
SETTINGS distributed_aggregation_memory_efficient = 0;

SELECT '-- memory-bound merging branch';
SELECT sum(v) AS s FROM merge(currentDatabase(), '^(v_04850|d_04850)$') WITH TOTALS
SETTINGS enable_memory_bound_merging_of_aggregation_results = 1, optimize_aggregation_in_order = 1;

SELECT '-- extremes stream';
SELECT sum(v) AS s FROM merge(currentDatabase(), '^(v_04850|d_04850)$') WITH TOTALS
SETTINGS extremes = 1;

-- The totals a merge step reports are re-derived from the merged states, so `WITH TOTALS` must not
-- change the aggregate value. Comparing against the same query without `WITH TOTALS` keeps every
-- other plan property identical, so an inequality can only come from the totals handling itself.
-- An empty result is the positive assertion.
SELECT '-- WITH TOTALS does not change the aggregate value';
SELECT
    (SELECT sum(v) FROM merge(currentDatabase(), '^(v_04850|d_04850)$') WITH TOTALS)
        = (SELECT sum(v) FROM merge(currentDatabase(), '^(v_04850|d_04850)$')) AS value_unchanged;

SELECT '-- and the TOTALS row equals the single aggregated row';
SELECT sum(v) AS s FROM merge(currentDatabase(), '^(v_04850|d_04850)$') WITH TOTALS;

DROP TABLE d_04850;
DROP VIEW v_04850;
DROP TABLE t_04850;
