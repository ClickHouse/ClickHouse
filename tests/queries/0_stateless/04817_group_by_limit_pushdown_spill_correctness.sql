-- Tags: no-parallel-replicas, long, no-sanitizers
-- Correctness of the `GROUP BY` top-K optimization when the aggregation
-- actually spills to disk.
--
-- Spilling is the case where a heap could silently lose data: the hash table
-- is flushed to a temporary file and started over many times, so a group whose
-- rows are spread across the whole stream is written out in several partial
-- states that only meet again in the external merge.  If the heap dropped rows
-- for a key it had already accepted into an earlier bucket - or if its boundary
-- were rebuilt from scratch after a flush and started rejecting rows of groups
-- already on disk - the merged group would come out incomplete, with a
-- perfectly plausible-looking value.
--
-- The workload is built so that late rows keep revisiting early groups: keys
-- cycle through `number % 500000` over 2000000 rows, so every key appears
-- exactly 4 times, spread evenly across the input and therefore across the
-- ~30 temporary parts the aggregation writes.  Any group that comes back with
-- a `sum(v)` other than 4 is an incomplete (or double-merged) group.
--
-- `LIMIT 10000` (well below `group_by_top_k_optimization_observation_rows`) is
-- what keeps both properties true at once: the heap fills early enough to
-- start pruning instead of freezing itself as pure overhead, while the retained
-- `1.5 * limit` groups still exceed the 1MB spill threshold.

SET optimize_trivial_group_by_limit_query = 0;
-- CI randomizes these; pin them so the heap engages and the spill is the only
-- thing under test.
SET query_plan_max_limit_for_top_k_optimization = 0;
SET group_by_top_k_optimization_load_factor = 1.5;
SET group_by_top_k_optimization_observation_rows = 65536;
SET max_rows_to_group_by = 0;
SET max_threads = 4;
SET group_by_two_level_threshold = 10000;
SET group_by_two_level_threshold_bytes = 50000000;
SET max_bytes_ratio_before_external_group_by = 0;

DROP TABLE IF EXISTS t_04817_ground_truth;
CREATE TABLE t_04817_ground_truth (k String, s UInt64) ENGINE = Memory;

-- The unspilled, unoptimized answer, materialized by its own statement.
-- `enable_group_by_top_k_optimization` takes effect per query, not per
-- subquery: inside one statement the last `SETTINGS` clause wins for the whole
-- query, so an on-versus-off comparison written as a single `EXCEPT` or `JOIN`
-- silently compares one mode against itself.
SET enable_group_by_top_k_optimization = 0;
SET max_bytes_before_external_group_by = 0;

INSERT INTO t_04817_ground_truth
SELECT k, sum(v)
FROM (SELECT toString(number % 500000) AS k, 1 AS v FROM numbers(2000000))
GROUP BY k;

SELECT 'ground truth groups', count(), min(s), max(s) FROM t_04817_ground_truth;

SET enable_group_by_top_k_optimization = 1;
SET max_bytes_before_external_group_by = 1000000;

-- `GROUP BY ... LIMIT N` without `ORDER BY`: the shape that has no coordinator
-- sort to discard a group the heap left incomplete, so every group it does
-- return must be whole.
SELECT 'no ORDER BY under spill: every returned group is complete';
SELECT count(), countIf(s = 4)
FROM
(
    SELECT k, sum(v) AS s
    FROM (SELECT toString(number % 500000) AS k, 1 AS v FROM numbers(2000000))
    GROUP BY k
    LIMIT 10000
) SETTINGS log_comment = '04817_no_order_by';

-- The keys themselves must be real keys of the full aggregation, with the same
-- aggregate value - a group invented or mis-merged by the spill path would not
-- join, dropping the match count below the LIMIT.
SELECT 'no ORDER BY under spill: groups agree with the unspilled aggregation';
SELECT count(), countIf(same)
FROM
(
    SELECT l.s = f.s AS same
    FROM
    (
        SELECT k, sum(v) AS s
        FROM (SELECT toString(number % 500000) AS k, 1 AS v FROM numbers(2000000))
        GROUP BY k
        LIMIT 10000
    ) AS l
    INNER JOIN t_04817_ground_truth AS f USING (k)
);

-- With `ORDER BY` the answer is fully determined, so the spilling optimized run
-- must be row-for-row equal to the unspilled unoptimized one.
SELECT 'ORDER BY under spill: identical to optimization off';
SELECT count()
FROM
(
    SELECT k, sum(v) AS s
    FROM (SELECT toString(number % 500000) AS k, 1 AS v FROM numbers(2000000))
    GROUP BY k ORDER BY k ASC LIMIT 10000
    EXCEPT
    SELECT k, s FROM t_04817_ground_truth ORDER BY k ASC LIMIT 10000
) SETTINGS log_comment = '04817_order_by';

-- Without this guard the queries above would still pass if the aggregation
-- never spilled or the heap froze itself - i.e. if they stopped testing
-- anything.
SYSTEM FLUSH LOGS query_log;

SELECT 'spill and heap both engaged';
SELECT
    sumIf(ProfileEvents['ExternalAggregationWritePart'], log_comment = '04817_no_order_by') > 0 AS no_order_by_spilled,
    sumIf(ProfileEvents['AggregationTopKRowsSkipped'], log_comment = '04817_no_order_by') > 0 AS no_order_by_pruned,
    sumIf(ProfileEvents['ExternalAggregationWritePart'], log_comment = '04817_order_by') > 0 AS order_by_spilled,
    sumIf(ProfileEvents['AggregationTopKRowsSkipped'], log_comment = '04817_order_by') > 0 AS order_by_pruned
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
    AND log_comment IN ('04817_no_order_by', '04817_order_by');

DROP TABLE t_04817_ground_truth;
