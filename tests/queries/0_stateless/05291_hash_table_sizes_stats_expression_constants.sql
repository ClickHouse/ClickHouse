-- The hash-table-stats cache key hashes the actions DAGs below the aggregation without the values
-- of their constants, so that a large folded constant is not hashed on every execution. The key
-- must still tell apart expressions that differ only in a literal: seen through a subquery the
-- aggregation key is named `__table1.k` in both queries below, so only the expression step separates
-- them. A query over a large folded scalar must keep a stable key, so its second run preallocates.
-- Constants are named by their value, so filters that differ only in a literal get distinct keys too.
--
-- Each run is checked through the `AggregationPreallocatedElementsInHashTables` profile event. The
-- group counts stay above the 500e3 lower bound under which `getSizeHint` does not preallocate.

SET enable_analyzer = 1;
SET max_threads = 1;
-- Checks the aggregation of this node; with parallel replicas it would run on the replicas.
SET enable_parallel_replicas = 0;
SET collect_hash_table_stats_during_aggregation = 1;
SET max_size_to_preallocate_for_aggregation = 1000000000000;
-- Spilling the aggregation stops the statistics collection.
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

DROP TABLE IF EXISTS t_05291;
CREATE TABLE t_05291 (v UInt64) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_05291 SELECT number FROM numbers(1e6);

SELECT k FROM (SELECT v % 600000 AS k FROM t_05291) GROUP BY k FORMAT Null SETTINGS log_comment = 'q05291_mod600k_1';
SELECT k FROM (SELECT v % 600000 AS k FROM t_05291) GROUP BY k FORMAT Null SETTINGS log_comment = 'q05291_mod600k_2';
-- Must not reuse the entry of the query above.
SELECT k FROM (SELECT v % 700000 AS k FROM t_05291) GROUP BY k FORMAT Null SETTINGS log_comment = 'q05291_mod700k_1';
SELECT k FROM (SELECT v % 700000 AS k FROM t_05291) GROUP BY k FORMAT Null SETTINGS log_comment = 'q05291_mod700k_2';

WITH (SELECT groupBitmapState(toUInt32(number)) FROM numbers(1e6)) AS bm
SELECT k FROM (SELECT v % 600000 + bitmapContains(bm, toUInt32(v)) AS k FROM t_05291) GROUP BY k FORMAT Null SETTINGS log_comment = 'q05291_bitmap_1';
WITH (SELECT groupBitmapState(toUInt32(number)) FROM numbers(1e6)) AS bm
SELECT k FROM (SELECT v % 600000 + bitmapContains(bm, toUInt32(v)) AS k FROM t_05291) GROUP BY k FORMAT Null SETTINGS log_comment = 'q05291_bitmap_2';

-- The same for a filter moved to PREWHERE, which the read step keys: 600000 groups, then 550000.
SELECT k FROM (SELECT v % 600000 AS k FROM t_05291 WHERE v < 700000) GROUP BY k FORMAT Null SETTINGS log_comment = 'q05291_prewhere700k_1';
SELECT k FROM (SELECT v % 600000 AS k FROM t_05291 WHERE v < 700000) GROUP BY k FORMAT Null SETTINGS log_comment = 'q05291_prewhere700k_2';
SELECT k FROM (SELECT v % 600000 AS k FROM t_05291 WHERE v < 550000) GROUP BY k FORMAT Null SETTINGS log_comment = 'q05291_prewhere550k_1';
SELECT k FROM (SELECT v % 600000 AS k FROM t_05291 WHERE v < 550000) GROUP BY k FORMAT Null SETTINGS log_comment = 'q05291_prewhere550k_2';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['AggregationPreallocatedElementsInHashTables']
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
    AND log_comment LIKE 'q05291\_%'
ORDER BY log_comment;

DROP TABLE t_05291;
