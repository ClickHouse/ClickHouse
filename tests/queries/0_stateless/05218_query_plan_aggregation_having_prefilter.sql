-- Tags: no-parallel-replicas
-- no-parallel-replicas: parallel replicas split the aggregation into partial + merging, so the
-- annotation's `isFinal()` requirement refuses and every applying cell below would return 0.

-- The `aggregationHavingPrefilter` plan optimization lets the final two-level bucket conversion skip
-- a group's key materialization when a HAVING bound on that aggregation's own no-argument `count()`
-- already rejects the group. The HAVING FilterStep stays in the plan and remains the authoritative
-- filter, so the cells below check which shapes carry the annotation, by searching the plan of
-- `EXPLAIN actions = 1` for the `HAVING pre-filter` line rather than printing whole plans, how many
-- groups each shape skips, and what a conversion that skips groups leaves observable above it.
-- The value oracles - the same rows with the setting off and on - and the dataflow statistics live in
-- `05234_query_plan_aggregation_having_prefilter_results`, which runs on the same fixture: a
-- sanitizer build has to finish either file inside one test's time budget.

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

SELECT '--- applies ---';

SELECT 'plain bound', count() FROM (EXPLAIN actions = 1
    SELECT a, b, c, count() AS cnt FROM having_prefilter GROUP BY a, b, c HAVING count() > 4
) WHERE explain LIKE '%HAVING pre-filter: count() > 4%';

SELECT 'mirrored bound', count() FROM (EXPLAIN actions = 1
    SELECT a, b, c, count() AS cnt FROM having_prefilter GROUP BY a, b, c HAVING 4 < count()
) WHERE explain LIKE '%HAVING pre-filter: count() > 4%';

SELECT 'mirrored greaterOrEquals', count() FROM (EXPLAIN actions = 1
    SELECT a, b, c, count() AS cnt FROM having_prefilter GROUP BY a, b, c HAVING 4 >= count()
) WHERE explain LIKE '%HAVING pre-filter: count() <= 4%';

SELECT 'mirrored lessOrEquals', count() FROM (EXPLAIN actions = 1
    SELECT a, b, c, count() AS cnt FROM having_prefilter GROUP BY a, b, c HAVING 4 <= count()
) WHERE explain LIKE '%HAVING pre-filter: count() >= 4%';

SELECT 'mirrored greater', count() FROM (EXPLAIN actions = 1
    SELECT a, b, c, count() AS cnt FROM having_prefilter GROUP BY a, b, c HAVING 4 > count()
) WHERE explain LIKE '%HAVING pre-filter: count() < 4%';

SELECT 'mirrored equals', count() FROM (EXPLAIN actions = 1
    SELECT a, b, c, count() AS cnt FROM having_prefilter GROUP BY a, b, c HAVING 4 = count()
) WHERE explain LIKE '%HAVING pre-filter: count() = 4%';

SELECT 'greaterOrEquals', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt >= 4
) WHERE explain LIKE '%HAVING pre-filter: count() >= 4%';

SELECT 'less', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt < 4
) WHERE explain LIKE '%HAVING pre-filter: count() < 4%';

SELECT 'lessOrEquals', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt <= 3
) WHERE explain LIKE '%HAVING pre-filter: count() <= 3%';

SELECT 'equals', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt = 4
) WHERE explain LIKE '%HAVING pre-filter: count() = 4%';

SELECT 'two count conjuncts', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3 AND cnt < 100
) WHERE explain LIKE '%HAVING pre-filter: count()%';

SELECT 'deterministic within the query beside the bound', count() FROM (EXPLAIN actions = 1
    SELECT a, c, count() AS cnt FROM having_prefilter GROUP BY a, c HAVING cnt > 3 AND c < today()
) WHERE explain LIKE '%HAVING pre-filter: count() > 3%';

SELECT 'count() not in the select list', count() FROM (EXPLAIN actions = 1
    SELECT a FROM having_prefilter GROUP BY a HAVING count() > 3
) WHERE explain LIKE '%HAVING pre-filter: count() > 3%';

-- `tryMergeFilters` folds the outer WHERE into the HAVING filter before this pass looks, so the pass
-- reads the merged expression: the bound survives as one conjunct of an `and` and is still a
-- necessary condition, so the annotation is applied to the merged shape.
SELECT 'outer WHERE on a group key, merged', count() FROM (EXPLAIN actions = 1
    SELECT a, cnt FROM (SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3) WHERE a != 'x'
) WHERE explain LIKE '%HAVING pre-filter: count() > 3%';

SELECT 'computed output beside the aggregate', count() FROM (EXPLAIN actions = 1
    SELECT a, cnt * 2 AS d FROM (SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3)
) WHERE explain LIKE '%HAVING pre-filter: count() > 3%';

-- The control for the side-effecting lambda in the refused section below: a lambda that captures the
-- aggregate is admitted on its own, so what that cell refuses is the side effect and not the capture.
SELECT 'pure lambda over the aggregate', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3 AND arrayExists(x -> x = cnt + 1, [1])
) WHERE explain LIKE '%HAVING pre-filter: count() > 3%';

-- `sleep` is the tree's only function declaring observable side effects, and its argument has to be
-- constant, so `pushDownFilter` always moves such a conjunct below the aggregation before this pass
-- looks: what is left above is the bound alone, and how many rows `sleep` runs on does not change.
SELECT 'side-effecting conjunct pushed below the aggregation', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3 AND sleep(0.001) = 0
) WHERE explain LIKE '%HAVING pre-filter: count() > 3%';

SELECT '--- refused ---';

SELECT 'setting off', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3
    SETTINGS query_plan_aggregation_having_prefilter = 0
) WHERE explain LIKE '%HAVING pre-filter%';

SELECT 'optimizations off', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3
    SETTINGS query_plan_enable_optimizations = 0
) WHERE explain LIKE '%HAVING pre-filter%';

-- `count(x)` may carry a nullable adapter, so its state is not the bare UInt64 the conversion reads.
SELECT 'count of an argument', count() FROM (EXPLAIN actions = 1
    SELECT a, count(nullIf(b, 12345)) AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3
) WHERE explain LIKE '%HAVING pre-filter%';

SELECT 'count distinct', count() FROM (EXPLAIN actions = 1
    SELECT a, count(DISTINCT b) AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3
) WHERE explain LIKE '%HAVING pre-filter%';

-- A materialized group has a count of at least one, so these keep every group and the test the
-- conversion would run per cell is pure overhead.
SELECT 'tautology > 0', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 0
) WHERE explain LIKE '%HAVING pre-filter%';

SELECT 'tautology >= 1', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt >= 1
) WHERE explain LIKE '%HAVING pre-filter%';

SELECT 'fractional bound', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 0.5
) WHERE explain LIKE '%HAVING pre-filter%';

SELECT 'negative bound', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > -1
) WHERE explain LIKE '%HAVING pre-filter%';

SELECT 'with totals', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a WITH TOTALS HAVING cnt > 3
) WHERE explain LIKE '%HAVING pre-filter%';

SELECT 'with rollup', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a WITH ROLLUP HAVING cnt > 3
) WHERE explain LIKE '%HAVING pre-filter%';

SELECT 'with cube', count() FROM (EXPLAIN actions = 1
    SELECT a, b, count() AS cnt FROM having_prefilter GROUP BY a, b WITH CUBE HAVING cnt > 3
) WHERE explain LIKE '%HAVING pre-filter%';

SELECT 'grouping sets', count() FROM (EXPLAIN actions = 1
    SELECT a, b, count() AS cnt FROM having_prefilter GROUP BY GROUPING SETS ((a), (b)) HAVING cnt > 3
) WHERE explain LIKE '%HAVING pre-filter%';

SELECT 'no group by', count() FROM (EXPLAIN actions = 1
    SELECT count() AS cnt FROM having_prefilter HAVING cnt > 3
) WHERE explain LIKE '%HAVING pre-filter%';

-- Neither side of an OR is a necessary condition on its own.
SELECT 'or of two bounds', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3 OR cnt < 2
) WHERE explain LIKE '%HAVING pre-filter%';

-- A pre-filtered input makes a stateful, non-deterministic or side-effecting filter expression see
-- different rows, and the retained filter cannot undo that: it is the thing being fed the shorter
-- input.
SELECT 'stateful conjunct', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3 AND rowNumberInAllBlocks() = 0
) WHERE explain LIKE '%HAVING pre-filter%';

SELECT 'stateful conjunct inside a lambda', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a
    HAVING cnt > 3 AND arrayExists(x -> x = rowNumberInAllBlocks(), [0])
) WHERE explain LIKE '%HAVING pre-filter%';

-- The capture is what keeps the conjunct above the aggregation, which is where a side-effecting
-- function could see the shorter input. `sleep` is the tree's only such function.
SELECT 'side-effecting conjunct inside a lambda over the aggregate', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a
    HAVING cnt > 3 AND arrayExists(x -> x = cnt + sleep(0.001), [1])
) WHERE explain LIKE '%HAVING pre-filter%';

SELECT 'non-deterministic conjunct', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3 AND rand() % 2 = 0
) WHERE explain LIKE '%HAVING pre-filter%';

-- The same, reading the aggregate, so `pushDownFilter` cannot move the conjunct below the
-- aggregation and the refusal is the only thing that can stop it.
SELECT 'non-deterministic conjunct over the aggregate', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3 AND rand(cnt) % 2 = 0
) WHERE explain LIKE '%HAVING pre-filter%';

SELECT 'array join in having', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3 AND arrayJoin([1, 1]) = 1
) WHERE explain LIKE '%HAVING pre-filter%';

-- The same class reached through the merge instead of written into the HAVING: the pass reads the
-- merged expression, so a stateful outer condition is refused rather than withdrawn later.
SELECT 'stateful outer condition, merged', count() FROM (EXPLAIN actions = 1
    SELECT a, cnt FROM (SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3)
    WHERE rowNumberInAllBlocks() = 0
) WHERE explain LIKE '%HAVING pre-filter%';

SELECT 'stateful outer condition across a projection, merged', count() FROM (EXPLAIN actions = 1
    SELECT a, d FROM (SELECT a, count() + 0 AS d FROM having_prefilter GROUP BY a HAVING count() > 3)
    WHERE rowNumberInAllBlocks() = 0
) WHERE explain LIKE '%HAVING pre-filter%';

-- These fields are not serialized, so a follower would run without them and EXPLAIN on the
-- initiator would advertise an optimization that does not happen.
SELECT 'serialized plan', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3
    SETTINGS serialize_query_plan = 1
) WHERE explain LIKE '%HAVING pre-filter%';

SELECT '--- the skipped-group count is 0 or exact for every operator, in both conversion paths ---';

-- A lone count() takes the inline `is_simple_count` conversion.
SELECT count(), sum(cnt) FROM (SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt >= 4)
    SETTINGS query_plan_aggregation_having_prefilter = 0, log_comment = '05218hp_ge_simple_off';
SELECT count(), sum(cnt) FROM (SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt >= 4)
    SETTINGS query_plan_aggregation_having_prefilter = 1, log_comment = '05218hp_ge_simple_on';
SELECT count(), sum(cnt) FROM (SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt < 4)
    SETTINGS query_plan_aggregation_having_prefilter = 0, log_comment = '05218hp_lt_simple_off';
SELECT count(), sum(cnt) FROM (SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt < 4)
    SETTINGS query_plan_aggregation_having_prefilter = 1, log_comment = '05218hp_lt_simple_on';

-- An aggregate that owns memory, listed before the count, so the general conversion runs, the count's
-- offset inside the cell is not zero, and the rejected groups' states are destroyed on the skip path.
SELECT count(), sum(cnt), sum(u) FROM (
    SELECT a, uniqExact(b) AS u, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt <= 3)
    SETTINGS query_plan_aggregation_having_prefilter = 0, log_comment = '05218hp_le_general_off';
SELECT count(), sum(cnt), sum(u) FROM (
    SELECT a, uniqExact(b) AS u, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt <= 3)
    SETTINGS query_plan_aggregation_having_prefilter = 1, log_comment = '05218hp_le_general_on';
-- `groupArray`'s state begins with the number of values it has collected, which equals the group's
-- count here, so it may not be the aggregate the count's offset is measured from.
SELECT count(), sum(cnt), sum(u), sum(length(g)) FROM (
    SELECT a, uniqExact(b) AS u, groupArray(c) AS g, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt = 4)
    SETTINGS query_plan_aggregation_having_prefilter = 0, log_comment = '05218hp_eq_general_off';
SELECT count(), sum(cnt), sum(u), sum(length(g)) FROM (
    SELECT a, uniqExact(b) AS u, groupArray(c) AS g, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt = 4)
    SETTINGS query_plan_aggregation_having_prefilter = 1, log_comment = '05218hp_eq_general_on';

-- A reversed-operand bound is recorded as the mirror of the written one, so the skipped count is the
-- one the mirrored op implies.
SELECT count(), sum(cnt) FROM (SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING 4 <= cnt)
    SETTINGS query_plan_aggregation_having_prefilter = 0, log_comment = '05218hp_ge_mirrored_off';
SELECT count(), sum(cnt) FROM (SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING 4 <= cnt)
    SETTINGS query_plan_aggregation_having_prefilter = 1, log_comment = '05218hp_ge_mirrored_on';

SYSTEM FLUSH LOGS query_log;

-- `tests/clickhouse-test` gives every query of this file its own `log_comment` of
-- `<test file name>-<database>`, so a `05218_` prefix here would also select those.
--
-- The elision is best effort: the pass may skip groups, it is never required to, and the retained
-- `FilterStep` stays authoritative. So a run whose configuration does not reach the two-level bucket
-- conversion skips nothing and is still correct - 1 of 50 runs of the private
-- `amd_asan_ubsan, flaky check, s3 storage, meta in keeper` job reported 0 for every cell below, while
-- the `EXPLAIN` cells above still carried the annotation. A skipped count is therefore accepted as 0 or
-- as the exact number of groups the operator rejects, and never as anything else: a wrong operator, a
-- mirrored bound mapped to the wrong operator, or a count read from the wrong aggregate's offset skips
-- a different, non-zero number of groups. The setting-off cells must skip nothing.
SELECT log_comment,
       ProfileEvents['AggregationHavingPrefilterGroupsSkipped'] IN (0, map(
           '05218hp_ge_simple_on', 4000, '05218hp_lt_simple_on', 2000, '05218hp_le_general_on', 2000,
           '05218hp_eq_general_on', 4000, '05218hp_ge_mirrored_on', 4000)[log_comment]) AS groups_skipped_ok
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND startsWith(log_comment, '05218hp_')
ORDER BY log_comment;

SELECT '--- block boundaries above the filter are unchanged ---';

-- `convertOneBucketToChunk` emits exactly one chunk per bucket however many groups survive, so a
-- block-observing function above the retained filter sees the same layout either way. `max_block_size`
-- is pinned small so a boundary would be visible at this group count, and `max_threads` because block
-- assignment across threads is not deterministic by itself.
SELECT count(), uniqExact(bn), max(rn), sum(rn) FROM (
    SELECT rowNumberInBlock() AS rn, blockNumber() AS bn FROM (
        SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3
    )
) SETTINGS query_plan_aggregation_having_prefilter = 0, max_threads = 1, max_block_size = 1000;
SELECT count(), uniqExact(bn), max(rn), sum(rn) FROM (
    SELECT rowNumberInBlock() AS rn, blockNumber() AS bn FROM (
        SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3
    )
) SETTINGS query_plan_aggregation_having_prefilter = 1, max_threads = 1, max_block_size = 1000;
SELECT count(), uniqExact(bn), max(rn), sum(rn) FROM (
    SELECT rowNumberInBlock() AS rn, blockNumber() AS bn FROM (
        SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3
    )
) SETTINGS query_plan_aggregation_having_prefilter = 0, max_threads = 1, max_block_size = 1000, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0;
SELECT count(), uniqExact(bn), max(rn), sum(rn) FROM (
    SELECT rowNumberInBlock() AS rn, blockNumber() AS bn FROM (
        SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3
    )
) SETTINGS query_plan_aggregation_having_prefilter = 1, max_threads = 1, max_block_size = 1000, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0;

SELECT '--- the group-by row limit still counts every group ---';

-- The limit is enforced from the bucket's true group count, filled before any conversion branch, so
-- a bound that rejects enough groups to bring the emitted rows under the limit must still throw.
SELECT count() FROM (
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3
) SETTINGS query_plan_aggregation_having_prefilter = 0, max_rows_to_group_by = 4000, group_by_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS }
SELECT count() FROM (
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3
) SETTINGS query_plan_aggregation_having_prefilter = 1, max_rows_to_group_by = 4000, group_by_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS }
SELECT count() FROM (
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3
) SETTINGS query_plan_aggregation_having_prefilter = 0, max_rows_to_group_by = 4000, group_by_overflow_mode = 'throw', group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0; -- { serverError TOO_MANY_ROWS }
SELECT count() FROM (
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING cnt > 3
) SETTINGS query_plan_aggregation_having_prefilter = 1, max_rows_to_group_by = 4000, group_by_overflow_mode = 'throw', group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0; -- { serverError TOO_MANY_ROWS }

SELECT '--- a throwing conjunct evaluated before the bound stops being reached ---';

-- Dropping a group before the filter runs elides the filter expression's evaluation on that group,
-- which a throwing conjunct can observe. ClickHouse already accepts and pins this class for plan
-- optimizations: `01655_plan_optimizations.sh:208-210` asserts that
-- `select throwIf(number = 5) from (select * from numbers(10)) order by number limit 1` returns `0`
-- even though row 5 exists.
--
-- Only the two deterministic halves are pinned as values: with the setting off the exception is
-- raised, and with the throwing conjunct written after the bound the conjunction is already lazy so
-- both arms agree. The remaining half - with the setting on and the throwing conjunct written before
-- the bound - is whether the annotated conversion elides the exception, and that is best effort: the
-- retained `FilterStep` is authoritative and the pass is only allowed to drop groups, never required
-- to, so a configuration that does not reach the annotated conversion raises again. It does happen:
-- 3 of 100 runs of the `s3 storage, meta in keeper` flaky check raised
-- `FUNCTION_THROW_IF_VALUE_IS_NON_ZERO` here while every other cell in this file matched. What is
-- deterministic is the plan, so that half is pinned as the annotation on the exact shape instead.
SELECT count() FROM (
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING throwIf(cnt = 3, 'boom') = 0 AND count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT 'throwing conjunct before the bound', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING throwIf(cnt = 3, 'boom') = 0 AND count() > 3
) WHERE explain LIKE '%HAVING pre-filter: count() > 3%';
SELECT count() FROM (
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING count() > 3 AND throwIf(cnt = 3, 'boom') = 0
) SETTINGS query_plan_aggregation_having_prefilter = 0;
SELECT count() FROM (
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING count() > 3 AND throwIf(cnt = 3, 'boom') = 0
) SETTINGS query_plan_aggregation_having_prefilter = 1;

-- Not specific to `throwIf`: an ordinary arithmetic error behaves the same. Written after the bound the
-- error is already elided without this setting, so only the first pair of arms diverges.
SELECT count() FROM (
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING intDiv(1, cnt - 3) > 0 AND count() > 3
) SETTINGS query_plan_aggregation_having_prefilter = 0; -- { serverError ILLEGAL_DIVISION }
SELECT 'dividing conjunct before the bound', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING intDiv(1, cnt - 3) > 0 AND count() > 3
) WHERE explain LIKE '%HAVING pre-filter: count() > 3%';
SELECT count() FROM (
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING count() > 3 AND intDiv(1, cnt - 3) > 0
) SETTINGS query_plan_aggregation_having_prefilter = 0;
SELECT count() FROM (
    SELECT a, count() AS cnt FROM having_prefilter GROUP BY a HAVING count() > 3 AND intDiv(1, cnt - 3) > 0
) SETTINGS query_plan_aggregation_having_prefilter = 1;

SELECT '--- a sibling aggregate that throws on a rejected group stops being reached ---';

-- A rejected group is neither filtered nor finalized, so an aggregate that throws from
-- `insertResultInto` on that group - `kolmogorovSmirnovTest` with one empty sample, here - stops
-- raising. This is the same elision the bucket Top-K conversion already performs by default on
-- master (`query_plan_aggregation_bucket_top_k`), which likewise destroys a rejected group's states
-- without finalizing them; the last cell pins that precedent next to this one, by its annotation. As above, the raising
-- arm is pinned as a value and the eliding arm as the annotation on the shape, because whether the
-- annotated conversion runs is not something this test can fix a configuration for.
DROP TABLE IF EXISTS having_prefilter_sibling;
CREATE TABLE having_prefilter_sibling (a UInt32, b Int64, s UInt8) ENGINE = MergeTree ORDER BY tuple();
-- Groups 0..1999 have four rows, one of them in the second sample; groups 2000..2999 have three rows,
-- all in the first, so only a group `HAVING cnt > 3` rejects has an empty sample.
INSERT INTO having_prefilter_sibling SELECT number % 3000, number, number >= 9000 FROM numbers(11000);

SELECT count(t.1) FROM (
    SELECT a, count() AS cnt, kolmogorovSmirnovTest(b, s) AS t FROM having_prefilter_sibling GROUP BY a HAVING cnt > 3
) SETTINGS query_plan_aggregation_having_prefilter = 0; -- { serverError BAD_ARGUMENTS }
SELECT 'sibling aggregate', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt, kolmogorovSmirnovTest(b, s) AS t FROM having_prefilter_sibling GROUP BY a HAVING cnt > 3
) WHERE explain LIKE '%HAVING pre-filter: count() > 3%';

-- The precedent is pinned the same way: whether the bucket Top-K conversion actually runs depends on
-- the configuration too (one run of the private `s3 storage, meta in keeper` flaky check reached the
-- exception with it enabled), so only its annotation on the shape is asserted, not the elision.
SELECT 'bucket Top-K precedent', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt, kolmogorovSmirnovTest(b, s) AS t FROM having_prefilter_sibling GROUP BY a ORDER BY cnt DESC LIMIT 5
    SETTINGS query_plan_aggregation_having_prefilter = 0, query_plan_aggregation_bucket_top_k = 1
) WHERE explain LIKE '%Bucket top-K: 5 descending%';

DROP TABLE having_prefilter_sibling;

SELECT '--- independent per-partition aggregation is refused ---';

-- `skip_merging` routes the pipeline through a squashing transform that re-packs the per-bucket
-- chunks by row and byte thresholds, so sparser chunks would land more buckets in one output block,
-- which `rowNumberInBlock` above the retained filter can see.
DROP TABLE IF EXISTS having_prefilter_parts;
CREATE TABLE having_prefilter_parts (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES having_prefilter_parts;
INSERT INTO having_prefilter_parts SELECT number % 1000, number FROM numbers_mt(10000);
INSERT INTO having_prefilter_parts SELECT number % 1000, number FROM numbers_mt(10000);

SELECT 'skip merging is set', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter_parts GROUP BY a HAVING cnt > 3
    SETTINGS allow_aggregate_partitions_independently = 1, force_aggregate_partitions_independently = 1, max_threads = 8
) WHERE explain LIKE '%Skip merging: 1%';

SELECT 'and the pre-filter is not', count() FROM (EXPLAIN actions = 1
    SELECT a, count() AS cnt FROM having_prefilter_parts GROUP BY a HAVING cnt > 3
    SETTINGS allow_aggregate_partitions_independently = 1, force_aggregate_partitions_independently = 1, max_threads = 8
) WHERE explain LIKE '%HAVING pre-filter%';

DROP TABLE having_prefilter_parts;
DROP TABLE having_prefilter;