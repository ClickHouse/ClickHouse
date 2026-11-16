-- Tags: stateful, long, no-msan

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000;

-- External aggregation is not supported as of now
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

-- Override randomized max_threads to avoid timeout on slow builds (ASan)
SET max_threads=0;

-- The runtime dataflow output-bytes estimate is sensitive to the block size, so pin
-- `max_block_size` to its default to keep the estimate stable against randomization.
SET max_block_size=65409;

-- The aggregation-state size estimate is recorded per bucket after the conversion to
-- a two-level hash table, so forcing the conversion from the very first block (the test
-- randomization sets these thresholds as low as 1) shifts the estimate well away from the
-- expected values calibrated under the default thresholds. Pin them to the defaults.
SET group_by_two_level_threshold=100000, group_by_two_level_threshold_bytes=50000000;

SELECT COUNT(*) FROM test.hits WHERE AdvEngineID <> 0 FORMAT Null SETTINGS log_comment='query_1';

-- Unsupported at the moment, refer to comments in `RuntimeDataflowStatisticsCacheUpdater::recordAggregationStateSizes`
-- SELECT COUNT(DISTINCT SearchPhrase) FROM test.hits FORMAT Null SETTINGS log_comment='query_5';

SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM test.hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_10';

SELECT SearchPhrase, COUNT(*) AS c FROM test.hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_12';

SELECT UserID, COUNT(*) FROM test.hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_15';

SELECT COUNT(*) FROM test.hits WHERE URL LIKE '%google%' FORMAT Null SETTINGS log_comment='query_20';

SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM test.hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_21';

SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM test.hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_22';

SELECT * FROM test.hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10 FORMAT Null SETTINGS log_comment='query_23';

SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM test.hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25 FORMAT Null SETTINGS log_comment='query_28';

SELECT 1, URL, COUNT(*) AS c FROM test.hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_34';

SELECT URL from test.hits WHERE URL LIKE '%yandex%' ORDER BY URL DESC FORMAT Null SETTINGS log_comment='query_43';

-- Unsupported case: filtering by set built from subquery
--SELECT * FROM test.hits WHERE CounterID IN (SELECT CounterID % 1000 FROM test.hits) FORMAT Null SETTINGS log_comment='query_44';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Just checking that the estimation is not too far off.
-- The `query_28` value was re-measured. The previously recorded 23722663 dates from 2025-12-31,
-- when the whole array was calibrated on the branch of the pull request that later merged as
-- "Introduce PackedStringRef & PackedStringHashTable"; merging it also overwrote the value master
-- itself had been green with since February, 31064320. Master has produced ~57..58 MB for this
-- query under the pinned settings at least since 2026-04-01: CI binaries of 2026-04-01, 2026-06-10,
-- and 2026-07-26 (before that merge) all measure 57.0..58.8 MB on this dataset, and nine runs in
-- the failing CI job spanned 57843408..59335657, a spread of 1.3%. So nothing regressed around the
-- merge - the estimate is stable, only the golden was stale. The median of those nine runs is
-- recorded here.
-- The ~58 MB is what `Aggregator::estimateSizeOfCompressedState` reports while it serializes the
-- sampled states into a `NullWriteBuffer` instead of into the `CompressedWriteBuffer` wrapped around
-- it, so the figure is the *uncompressed* serialized size of the `MIN(Referer)` states rather than
-- their size on the wire. That is a defect of the estimator, not of this test, and it is what makes
-- the estimate overshoot the actually transferred bytes by ~2.45x for this query.
-- Once the estimator measures the compressed size, this value has to be re-measured again - it goes
-- back to about the previously recorded 23722663.
WITH
    [3, 195461, 5962954, 1100491, 2, 16885, 42323, 9434, 58136394, 203701090, 82404720/*, 641835*/] AS expected_bytes,
    arrayJoin(arrayMap(x -> (untuple(x.1), x.2), arrayZip(res, expected_bytes))) AS res
SELECT format('{} {} {}', res.1, res.2, res.3)
FROM
(
    SELECT groupArray((log_comment, output_bytes)) AS res
    FROM (
      SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] output_bytes
      FROM system.query_log
      WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE 'query_%') AND (type = 'QueryFinish')
      ORDER BY event_time_microseconds
    )
)
WHERE (greatest(res.2, res.3) / least(res.2, res.3)) > 2.5 AND NOT (res.2 < 100 AND res.3 < 100);

