-- Tags: no-parallel-replicas
-- no-parallel-replicas: parallel replicas split the aggregation into partial + merging, so the
-- annotation's `isFinal()` requirement refuses and every applying cell below would return 0.

-- The `aggregationHavingPrefilter` plan optimization lets the final two-level bucket conversion skip
-- a group's key materialization when a HAVING bound on that aggregation's own no-argument `count()`
-- already rejects the group. The HAVING FilterStep stays in the plan and remains the authoritative
-- filter, so the cells below check that the rows are identical with the setting off and on, and that
-- the runtime dataflow statistics a skipping conversion reports are the ones an ordinary run reports.
-- Which shapes carry the annotation is checked in
-- `05218_query_plan_aggregation_having_prefilter`, which runs on the same fixture: a sanitizer build
-- has to finish either file inside one test's time budget.

-- The pass refuses a serialized plan, so pin the setting for the distributed-plan suite.
SET serialize_query_plan = 0;
SET query_plan_enable_optimizations = 1;
SET query_plan_aggregation_having_prefilter = 1;
-- The annotation is only honoured in the two-level bucket conversion; pin both thresholds so that
-- path is taken whatever the runner randomizes.
SET group_by_two_level_threshold = 100;
SET group_by_two_level_threshold_bytes = 1;
-- Those thresholds are zeroed again unless the pipeline has more than one stream or an external
-- group-by threshold is set (AggregatingStep::transformPipeline), and this fixture is small enough to
-- be read in one stream. Pin a threshold far above it: non-zero keeps the two-level path, and nothing
-- here comes close to spilling.
SET max_bytes_before_external_group_by = 10737418240;
SET optimize_aggregation_in_order = 0;
SET query_plan_merge_filters = 1;
-- Some CI configurations set this as a safety net, which would make the group-by-limit cells at the
-- end throw for a reason of their own.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS having_prefilter;
CREATE TABLE having_prefilter (a String, b Int64, c Date) ENGINE = MergeTree ORDER BY tuple();
-- 6000 groups over 20000 rows: keys 0..1999 have count 4 and keys 2000..5999 have count 3, so
-- `HAVING count() > 3` keeps exactly 2000 groups. The fixture is kept small because the file runs
-- dozens of aggregations over it, and a sanitizer build has to finish all of them. The key is a
-- 30-byte String plus an Int64 and a Date, which is the wide `serialized` method where the skipped
-- materialization is worth most.
INSERT INTO having_prefilter
SELECT concat('key-padding-to-thirty-bytes--', toString(number % 6000)),
       toInt64(number % 6000),
       toDate('2020-01-01') + ((number % 6000) % 30)
FROM numbers(20000);

SELECT '--- results are unchanged ---';

-- The three-key `serialized` fixture: with the setting off and on, the same 10000 groups.
SELECT count(), sum(cnt), min(cnt), max(cnt) FROM (
    SELECT a, b, c, count() AS cnt FROM having_prefilter GROUP BY a, b, c HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 0;
SELECT count(), sum(cnt), min(cnt), max(cnt) FROM (
    SELECT a, b, c, count() AS cnt FROM having_prefilter GROUP BY a, b, c HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 1;

-- All three keys Nullable with no NULL present: the reported TPC-DS shape.
SELECT count(), sum(cnt) FROM (
    SELECT toNullable(a) AS na, toNullable(b) AS nb, toNullable(c) AS nc, count() AS cnt
    FROM having_prefilter GROUP BY na, nb, nc HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 0;
SELECT count(), sum(cnt) FROM (
    SELECT toNullable(a) AS na, toNullable(b) AS nb, toNullable(c) AS nc, count() AS cnt
    FROM having_prefilter GROUP BY na, nb, nc HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 1;

-- A NULL-keyed group lives outside the hash-table cells, so it is never pre-filtered and has to be
-- removed by the filter above. Once where it passes the bound, once where it fails.
SELECT count(), sum(cnt), countIf(k IS NULL) FROM (
    SELECT nullIf(b, -1) % 6000 AS k, count() AS cnt FROM (
        SELECT b FROM having_prefilter UNION ALL SELECT NULL FROM numbers(5)
    ) GROUP BY k HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 0;
SELECT count(), sum(cnt), countIf(k IS NULL) FROM (
    SELECT nullIf(b, -1) % 6000 AS k, count() AS cnt FROM (
        SELECT b FROM having_prefilter UNION ALL SELECT NULL FROM numbers(5)
    ) GROUP BY k HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 1;
SELECT count(), sum(cnt), countIf(k IS NULL) FROM (
    SELECT nullIf(b, -1) % 6000 AS k, count() AS cnt FROM (
        SELECT b FROM having_prefilter UNION ALL SELECT NULL FROM numbers(2)
    ) GROUP BY k HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 0;
SELECT count(), sum(cnt), countIf(k IS NULL) FROM (
    SELECT nullIf(b, -1) % 6000 AS k, count() AS cnt FROM (
        SELECT b FROM having_prefilter UNION ALL SELECT NULL FROM numbers(2)
    ) GROUP BY k HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 1;

-- A single Nullable key is `one_key_nullable_optimization`, and a LowCardinality key clears the
-- simple-count path: both are covered rather than excluded.
SELECT count(), sum(cnt) FROM (
    SELECT toNullable(b) AS k, count() AS cnt FROM having_prefilter GROUP BY k HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 0;
SELECT count(), sum(cnt) FROM (
    SELECT toNullable(b) AS k, count() AS cnt FROM having_prefilter GROUP BY k HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 1;
SELECT count(), sum(cnt) FROM (
    SELECT toLowCardinality(a) AS k, count() AS cnt FROM having_prefilter GROUP BY k HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 0;
SELECT count(), sum(cnt) FROM (
    SELECT toLowCardinality(a) AS k, count() AS cnt FROM having_prefilter GROUP BY k HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 1;

-- A second aggregate whose state owns memory: the rejected groups' states are destroyed inline, so a
-- double destroy or a leak would show here first (and under a sanitizer build).
SELECT count(), sum(cnt), sum(u), sum(length(g)) FROM (
    SELECT a, count() AS cnt, uniqExact(b) AS u, groupArray(c) AS g
    FROM having_prefilter GROUP BY a HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 0;
SELECT count(), sum(cnt), sum(u), sum(length(g)) FROM (
    SELECT a, count() AS cnt, uniqExact(b) AS u, groupArray(c) AS g
    FROM having_prefilter GROUP BY a HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 1;

-- The same shape with a bound that keeps nothing: every state is destroyed on the rejected path.
SELECT count(), sum(cnt), sum(u) FROM (
    SELECT a, count() AS cnt, uniqExact(b) AS u, groupArray(c) AS g
    FROM having_prefilter GROUP BY a HAVING count() > 1000000
) SETTINGS query_plan_aggregation_having_prefilter = 0;
SELECT count(), sum(cnt), sum(u) FROM (
    SELECT a, count() AS cnt, uniqExact(b) AS u, groupArray(c) AS g
    FROM having_prefilter GROUP BY a HAVING count() > 1000000
) SETTINGS query_plan_aggregation_having_prefilter = 1;

SELECT count(), sum(cnt) FROM (
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 0, group_by_use_nulls = 1;
SELECT count(), sum(cnt) FROM (
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 1, group_by_use_nulls = 1;

-- External aggregation spills with `final = false`, so the pre-filter is excluded there; the pair
-- proves the answer is unchanged on a path it never reaches.
SELECT count(), sum(cnt) FROM (
    SELECT a, b, c, count() AS cnt FROM having_prefilter GROUP BY a, b, c HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 0, max_bytes_before_external_group_by = 1000000, max_bytes_ratio_before_external_group_by = 0;
SELECT count(), sum(cnt) FROM (
    SELECT a, b, c, count() AS cnt FROM having_prefilter GROUP BY a, b, c HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 1, max_bytes_before_external_group_by = 1000000, max_bytes_ratio_before_external_group_by = 0;

-- No external threshold over a single-stream read leaves the aggregation single-level whatever the
-- two-level thresholds say, so the annotation is applied and never acted on. Unchanged answer.
SELECT count(), sum(cnt) FROM (
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SELECT count(), sum(cnt) FROM (
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 1, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

-- Single-level: the annotation is applied but the conversion never opts in, so the pair proves the
-- answer is unchanged rather than that the path fired.
SELECT count(), sum(cnt) FROM (
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 0, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0;
SELECT count(), sum(cnt) FROM (
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 1, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0;

-- The merged shape is a supported path, so it needs a value oracle and not only the gate above.
SELECT count(), sum(cnt) FROM (
    SELECT a, cnt FROM (SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3) WHERE a != 'x'
) SETTINGS query_plan_aggregation_having_prefilter = 0;
SELECT count(), sum(cnt) FROM (
    SELECT a, cnt FROM (SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3) WHERE a != 'x'
) SETTINGS query_plan_aggregation_having_prefilter = 1;

SELECT '--- the pre-filter is applied while dataflow statistics are collected ---';

-- `automatic_parallel_replicas_mode = 2` attaches a dataflow statistics updater to the aggregation and
-- collects statistics only, never switching the plan to parallel replicas, so the aggregation stays
-- final and the annotation is still honoured. The conversion keeps skipping the rejected groups and
-- reports the untruncated key sizes to the estimator, so a query behaves the same whether or not the
-- statistics cache is being filled.
-- The three queries are deliberately not wrapped in a subquery: the optimization matches the plan node
-- of the top-level aggregation, and with a wrapping subquery it skips the plan without attaching an
-- updater, which would leave every cell below measuring an ordinary run.
-- The estimate cell below compares byte counts the two runs arrive at by different routes - the
-- pre-filtered run reports the keys measured on the hash table, the ordinary one the materialized
-- chunk - so it needs enough groups for the sampled compression ratio to settle. This is the only
-- section that does, so it gets its own larger fixture rather than making the whole file run on one.
DROP TABLE IF EXISTS having_prefilter_wide;
-- `d` is a stored `LowCardinality` column rather than an expression over the other keys, because a key
-- that is a function of another key is dropped from the GROUP BY by `optimize_group_by_function_keys`
-- and the repeated-key cell below would then measure nothing.
CREATE TABLE having_prefilter_wide (a String, b Int64, c Date, d LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO having_prefilter_wide
SELECT concat('key-padding-to-thirty-bytes--', toString(number % 30000)),
       toInt64(number % 30000),
       toDate('2020-01-01') + ((number % 30000) % 30),
       concat('padding-to-thirty-bytes-wide--', toString(number % 30))
FROM numbers(100000);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 2, parallel_replicas_local_plan = 1,
    parallel_replicas_index_analysis_only_on_coordinator = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas';

SELECT a, count() AS cnt FROM having_prefilter_wide GROUP BY a HAVING count() > 3 FORMAT Null
    SETTINGS query_plan_aggregation_having_prefilter = 1, log_comment = '05233ap_on';
SELECT a, count() AS cnt FROM having_prefilter_wide GROUP BY a HAVING count() > 3 FORMAT Null
    SETTINGS query_plan_aggregation_having_prefilter = 0, log_comment = '05233ap_off';
SELECT a, count() AS cnt FROM having_prefilter_wide GROUP BY a HAVING throwIf(cnt = 3, 'boom') = 0 AND count() > 3 FORMAT Null
    SETTINGS query_plan_aggregation_having_prefilter = 1, log_comment = '05233ap_throwif';

-- A bound no group meets empties every bucket's chunk. The statistics divide the accumulated key bytes
-- by a compression ratio sampled from the chunk, so with nothing left there to sample the bytes would be
-- dropped and the shipping term underpriced; the conversion keeps a bounded sample of the keys it
-- measured for this case, which is what the estimate cell below reads.
SELECT a, count() AS cnt FROM having_prefilter_wide GROUP BY a HAVING count() > 1000000 FORMAT Null
    SETTINGS query_plan_aggregation_having_prefilter = 1, log_comment = '05233ap_none_on';
SELECT a, count() AS cnt FROM having_prefilter_wide GROUP BY a HAVING count() > 1000000 FORMAT Null
    SETTINGS query_plan_aggregation_having_prefilter = 0, log_comment = '05233ap_none_off';

-- A high-cardinality `LowCardinality` key: the meter's scratch row is one of those columns, and
-- `ColumnLowCardinality::popBack` leaves the value it measured interned in the dictionary, so the
-- scratch column is rebuilt once it outgrows its bound. The estimate has to survive that rebuild.
SELECT toLowCardinality(a) AS k, count() AS cnt FROM having_prefilter_wide GROUP BY k HAVING count() > 3 FORMAT Null
    SETTINGS query_plan_aggregation_having_prefilter = 1, log_comment = '05233ap_lc_on';
SELECT toLowCardinality(a) AS k, count() AS cnt FROM having_prefilter_wide GROUP BY k HAVING count() > 3 FORMAT Null
    SETTINGS query_plan_aggregation_having_prefilter = 0, log_comment = '05233ap_lc_off';

-- The same `LowCardinality` value repeated across many groups - 30 distinct values over 30000 groups.
-- The meter measures a key by how much the column it goes into grows, so this value is charged its
-- index in every group and its dictionary entry once, the way the materialized column carries it.
-- Summing `byteSizeAt` instead - which reports the referenced dictionary value alone - charges the
-- whole 30-byte value once per group and drops every row's index: 1430000 bytes against the 575426 the
-- materialized column reports, which is what the cell below would see.
SELECT d, b, count() AS cnt
FROM having_prefilter_wide GROUP BY d, b HAVING count() > 3 FORMAT Null
    SETTINGS query_plan_aggregation_having_prefilter = 1, log_comment = '05233ap_lcrep_on';
SELECT d, b, count() AS cnt
FROM having_prefilter_wide GROUP BY d, b HAVING count() > 3 FORMAT Null
    SETTINGS query_plan_aggregation_having_prefilter = 0, log_comment = '05233ap_lcrep_off';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

DROP TABLE having_prefilter_wide;

SYSTEM FLUSH LOGS query_log;

-- `statistics_collected` is what keeps these cells honest: without an attached updater the ordinary
-- pre-filter would produce the same skipped-group counts and suppress the same exception, and every
-- cell here would pass while measuring nothing.
SELECT log_comment,
       ProfileEvents['AggregationHavingPrefilterGroupsSkipped'] AS groups_skipped,
       ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS statistics_collected
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time > now() - INTERVAL 15 MINUTE
  AND current_database = currentDatabase() AND startsWith(log_comment, '05233ap_')
ORDER BY log_comment;

-- The keys reported are the whole bucket's, so the estimate of the bytes replicas would ship is the one
-- the run without the pre-filter reports. Reporting only the materialized groups' keys would lose about
-- half of it. The bound is not equality because the compression ratio the estimate divides by is sampled
-- from a subset of the buckets, and which ones depends on the order they are merged in.
SELECT 'estimate matches the run without the pre-filter',
       greatest(on_bytes, off_bytes) <= least(on_bytes, off_bytes) * 1.25 AS ok
FROM
(
    SELECT
        anyIf(ProfileEvents['RuntimeDataflowStatisticsOutputBytes'], log_comment = '05233ap_on') AS on_bytes,
        anyIf(ProfileEvents['RuntimeDataflowStatisticsOutputBytes'], log_comment = '05233ap_off') AS off_bytes
    FROM system.query_log
    WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time > now() - INTERVAL 15 MINUTE
      AND current_database = currentDatabase() AND log_comment IN ('05233ap_on', '05233ap_off')
);

-- The same comparison for the two runs above: an all-rejected aggregation still has to report the keys
-- its groups would have shipped, and a `LowCardinality` key still has to report them after the scratch
-- column is rebuilt. Without either, the pre-filtered run loses the whole `AggregationKeys` term.
SELECT 'estimate matches with every group rejected',
       greatest(on_bytes, off_bytes) <= least(on_bytes, off_bytes) * 1.25 AS ok
FROM
(
    SELECT
        anyIf(ProfileEvents['RuntimeDataflowStatisticsOutputBytes'], log_comment = '05233ap_none_on') AS on_bytes,
        anyIf(ProfileEvents['RuntimeDataflowStatisticsOutputBytes'], log_comment = '05233ap_none_off') AS off_bytes
    FROM system.query_log
    WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time > now() - INTERVAL 15 MINUTE
      AND current_database = currentDatabase() AND log_comment IN ('05233ap_none_on', '05233ap_none_off')
);

SELECT 'estimate matches for a repeated LowCardinality key',
       greatest(on_bytes, off_bytes) <= least(on_bytes, off_bytes) * 1.25 AS ok
FROM
(
    SELECT
        anyIf(ProfileEvents['RuntimeDataflowStatisticsOutputBytes'], log_comment = '05233ap_lcrep_on') AS on_bytes,
        anyIf(ProfileEvents['RuntimeDataflowStatisticsOutputBytes'], log_comment = '05233ap_lcrep_off') AS off_bytes
    FROM system.query_log
    WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time > now() - INTERVAL 15 MINUTE
      AND current_database = currentDatabase() AND log_comment IN ('05233ap_lcrep_on', '05233ap_lcrep_off')
);

SELECT 'estimate matches for a LowCardinality key',
       greatest(on_bytes, off_bytes) <= least(on_bytes, off_bytes) * 1.25 AS ok
FROM
(
    SELECT
        anyIf(ProfileEvents['RuntimeDataflowStatisticsOutputBytes'], log_comment = '05233ap_lc_on') AS on_bytes,
        anyIf(ProfileEvents['RuntimeDataflowStatisticsOutputBytes'], log_comment = '05233ap_lc_off') AS off_bytes
    FROM system.query_log
    WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time > now() - INTERVAL 15 MINUTE
      AND current_database = currentDatabase() AND log_comment IN ('05233ap_lc_on', '05233ap_lc_off')
);

DROP TABLE having_prefilter;
