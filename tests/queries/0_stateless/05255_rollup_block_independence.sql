-- Tests that GROUP BY ... WITH ROLLUP returns the same rows no matter how the input is split into
-- blocks. Each case compares a many-block run against a one-block run of the same query, over a
-- multiset checksum so the comparison ignores row order but not a missing row. The last sections
-- read profile events and a plan line, so that which runs received several blocks at all, which of
-- them skipped the input merge, and which GROUP BY algorithm produced them, is checked and not
-- assumed. Runs 2 and 3 there check that an aggregate whose result depends on the merge order keeps
-- today's merge.

-- The settings every run shares are set once below, so that each query carries only what makes it
-- the case it is and the two arms of a comparison can be seen to differ only in how their input is
-- split. The large `max_bytes_before_external_group_by` is one of them and must stay non-zero:
-- two-level aggregation, which is what splits the input of a single reading stream into several
-- blocks, is only offered to a query that may spill. The limit itself is far above anything 30000
-- rows can reach, so no run spills, and the profile events check that rather than assuming it.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS rollup_blocks;
DROP TABLE IF EXISTS rollup_partitioned;

CREATE TABLE rollup_blocks
(
    a UInt16, b UInt16, c UInt16,
    v Int64, f Float64, af Array(Float64), lc LowCardinality(Nullable(Int64))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO rollup_blocks SELECT
    number % 7, number % 11, number % 13,
    number, number / 3, [number / 5, number / 9], if(number % 17 = 0, NULL, toInt64(number))
FROM numbers(30000);

-- Each partition holds exactly one value of `a`, so the aggregation may run per partition without a
-- cross-stream merge and still give every group key to one stream only. Eight partitions also mean
-- eight parts, which is what lets a query over this table read on several threads at once.
CREATE TABLE rollup_partitioned (a UInt16, b UInt16, v Int64)
ENGINE = MergeTree PARTITION BY a ORDER BY tuple();

INSERT INTO rollup_partitioned SELECT number % 8, number % 11, number FROM numbers(30000);

-- `max_threads` and the two external-aggregation limits are pinned because the test runner
-- randomizes them. The two `group_by_two_level_threshold*` settings are pinned so that only the
-- per-query one below decides whether the input is split, and the concurrent-read limits so that
-- how many threads read a table is decided by its part count alone, on any disk. The one-block runs
-- use one thread: several threads reading the eight-part table would aggregate its partitions
-- independently, or adaptively, and either way hand the rollup more than one block. The test
-- profile sets `max_rows_to_group_by`, and any such limit stops partitions being aggregated
-- independently, so it is reset.
SET max_threads = 4;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET max_bytes_before_external_group_by = 1000000000;
SET max_bytes_ratio_before_external_group_by = 0;
SET group_by_two_level_threshold_bytes = 0;
SET merge_tree_min_rows_for_concurrent_read = 163840;
SET merge_tree_min_rows_for_concurrent_read_for_remote_filesystem = 163840;
SET merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem = 251658240;

SELECT 'rollup, three keys', count(), sum(cityHash64(toString((a, b, c, s, n))))
FROM (
    SELECT a, b, c, sum(v) AS s, count() AS n FROM rollup_blocks GROUP BY a, b, c WITH ROLLUP
    SETTINGS max_block_size = 100, group_by_two_level_threshold = 1
);

SELECT 'rollup, two keys, small result', a, b, sum(v), count(), min(a), uniqExact(v)
FROM rollup_blocks WHERE a < 2 AND b < 2 GROUP BY a, b WITH ROLLUP ORDER BY ALL
SETTINGS max_block_size = 100, group_by_two_level_threshold = 1;

SELECT 'many blocks = one block, three keys', (
    SELECT sum(cityHash64(toString((a, b, c, s, n)))) FROM (
        SELECT a, b, c, sum(v) AS s, count() AS n FROM rollup_blocks GROUP BY a, b, c WITH ROLLUP
        SETTINGS max_block_size = 100, group_by_two_level_threshold = 1)
) = (
    SELECT sum(cityHash64(toString((a, b, c, s, n)))) FROM (
        SELECT a, b, c, sum(v) AS s, count() AS n FROM rollup_blocks GROUP BY a, b, c WITH ROLLUP
        SETTINGS max_threads = 1, max_block_size = 1000000, group_by_two_level_threshold = 0)
);

SELECT 'many blocks = one block, no aggregate functions', (
    SELECT sum(cityHash64(toString((a, b)))) FROM (
        SELECT a, b FROM rollup_blocks GROUP BY a, b WITH ROLLUP
        SETTINGS max_block_size = 100, group_by_two_level_threshold = 1)
) = (
    SELECT sum(cityHash64(toString((a, b)))) FROM (
        SELECT a, b FROM rollup_blocks GROUP BY a, b WITH ROLLUP
        SETTINGS max_threads = 1, max_block_size = 1000000, group_by_two_level_threshold = 0)
);

SELECT 'many blocks = one block, null keys for sub-totals', (
    SELECT sum(cityHash64(toString((a, b, s)))) FROM (
        SELECT a, b, sum(v) AS s FROM rollup_blocks GROUP BY a, b WITH ROLLUP
        SETTINGS group_by_use_nulls = 1, max_block_size = 100, group_by_two_level_threshold = 1)
) = (
    SELECT sum(cityHash64(toString((a, b, s)))) FROM (
        SELECT a, b, sum(v) AS s FROM rollup_blocks GROUP BY a, b WITH ROLLUP
        SETTINGS group_by_use_nulls = 1, max_threads = 1,
                 max_block_size = 1000000, group_by_two_level_threshold = 0)
);

SELECT 'many blocks = one block, condition and low cardinality arguments', (
    SELECT sum(cityHash64(toString((a, b, s, l)))) FROM (
        SELECT a, b, sumIf(v, c > 5) AS s, sum(lc) AS l FROM rollup_blocks GROUP BY a, b WITH ROLLUP
        SETTINGS max_block_size = 100, group_by_two_level_threshold = 1)
) = (
    SELECT sum(cityHash64(toString((a, b, s, l)))) FROM (
        SELECT a, b, sumIf(v, c > 5) AS s, sum(lc) AS l FROM rollup_blocks GROUP BY a, b WITH ROLLUP
        SETTINGS max_threads = 1, max_block_size = 1000000, group_by_two_level_threshold = 0)
);

SELECT 'many blocks = one block, a key repeated in the group by list', (
    SELECT sum(cityHash64(toString((a, b, s)))) FROM (
        SELECT a, b, sum(v) AS s FROM rollup_blocks GROUP BY a, b, a WITH ROLLUP
        SETTINGS max_block_size = 100, group_by_two_level_threshold = 1)
) = (
    SELECT sum(cityHash64(toString((a, b, s)))) FROM (
        SELECT a, b, sum(v) AS s FROM rollup_blocks GROUP BY a, b, a WITH ROLLUP
        SETTINGS max_threads = 1, max_block_size = 1000000, group_by_two_level_threshold = 0)
);

SELECT 'many blocks = one block, array and map keys', (
    SELECT sum(cityHash64(toString((af, m, s)))) FROM (
        SELECT af, map('k', a) AS m, sum(v) AS s FROM rollup_blocks GROUP BY af, m WITH ROLLUP
        SETTINGS max_block_size = 100, group_by_two_level_threshold = 1)
) = (
    SELECT sum(cityHash64(toString((af, m, s)))) FROM (
        SELECT af, map('k', a) AS m, sum(v) AS s FROM rollup_blocks GROUP BY af, m WITH ROLLUP
        SETTINGS max_threads = 1, max_block_size = 1000000, group_by_two_level_threshold = 0)
);

-- `allow_aggregate_partitions_independently` is set on the statement in the next two cases, because
-- a subquery's `SETTINGS` do not reach the plan optimization that reads it.
SELECT 'many blocks = one block, independently aggregated partitions', (
    SELECT sum(cityHash64(toString((a, b, s, n)))) FROM (
        SELECT a, b, sum(v) AS s, count() AS n FROM rollup_partitioned GROUP BY a, b WITH ROLLUP
        SETTINGS force_aggregate_partitions_independently = 1,
                 max_block_size = 100, group_by_two_level_threshold = 1)
) = (
    SELECT sum(cityHash64(toString((a, b, s, n)))) FROM (
        SELECT a, b, sum(v) AS s, count() AS n FROM rollup_partitioned GROUP BY a, b WITH ROLLUP
        SETTINGS max_threads = 1, max_block_size = 1000000, group_by_two_level_threshold = 0)
) SETTINGS allow_aggregate_partitions_independently = 1;

-- The adaptive GROUP BY, which is on by default, is a second way to produce an input whose group
-- keys each land in one block only: every thread stops growing its own table at
-- `adaptive_aggregator_freeze_threshold` keys and the keys it sees afterwards are aggregated once
-- during the merge instead. It needs several reading threads, so this is the eight-part table, and
-- grouping by `v` makes almost every key distinct so that the algorithm does not hand the threads
-- back to the ordinary one for a stream of repeats.
SELECT 'many blocks = one block, adaptive aggregation', (
    SELECT sum(cityHash64(toString((v, a, s, n)))) FROM (
        SELECT v, a, sum(v) AS s, count() AS n FROM rollup_partitioned GROUP BY v, a WITH ROLLUP
        SETTINGS adaptive_aggregator_freeze_threshold = 0,
                 max_block_size = 100, group_by_two_level_threshold = 1)
) = (
    SELECT sum(cityHash64(toString((v, a, s, n)))) FROM (
        SELECT v, a, sum(v) AS s, count() AS n FROM rollup_partitioned GROUP BY v, a WITH ROLLUP
        SETTINGS adaptive_aggregator_freeze_threshold = 0,
                 max_threads = 1, max_block_size = 1000000, group_by_two_level_threshold = 0)
) SETTINGS allow_aggregate_partitions_independently = 0;

SELECT 'rollup with totals, two keys', a, b, sum(v) FROM rollup_blocks
WHERE a < 2 AND b < 2 GROUP BY a, b WITH ROLLUP WITH TOTALS ORDER BY ALL
SETTINGS max_block_size = 100, group_by_two_level_threshold = 1;

SELECT 'cube, two keys', a, b, sum(v), count() FROM rollup_blocks
WHERE a < 2 AND b < 2 GROUP BY a, b WITH CUBE ORDER BY ALL
SETTINGS max_block_size = 100, group_by_two_level_threshold = 1;

-- Each run below carries its own log comment, so the profile events can be read back per run. Runs
-- 1, 5 and 6 receive several blocks and only exactly merging aggregates, so they report the skipped
-- merge; 2 and 3 hold an aggregate that does not merge exactly, and 4 receives a single block. The
-- second column says whether the run split its input at all, which is what makes 2 and 3 reporting
-- nothing a statement about the aggregate rather than about the input; the third says which runs
-- used the adaptive algorithm; the fourth says that no run reached the external aggregation limit.

SELECT a, b, sum(v) FROM rollup_blocks GROUP BY a, b WITH ROLLUP
SETTINGS log_comment = '05255-skip-1-sum-int64-many-blocks', log_queries = 1,
         max_block_size = 100, group_by_two_level_threshold = 1
FORMAT Null;

SELECT a, b, argMax(v, c) FROM rollup_blocks GROUP BY a, b WITH ROLLUP
SETTINGS log_comment = '05255-skip-2-argmax-many-blocks', log_queries = 1,
         max_block_size = 100, group_by_two_level_threshold = 1
FORMAT Null;

SELECT a, b, sum(f) FROM rollup_blocks GROUP BY a, b WITH ROLLUP
SETTINGS log_comment = '05255-skip-3-sum-float-many-blocks', log_queries = 1,
         max_block_size = 100, group_by_two_level_threshold = 1
FORMAT Null;

SELECT a, b, sum(v) FROM rollup_blocks GROUP BY a, b WITH ROLLUP
SETTINGS log_comment = '05255-skip-4-sum-int64-one-block', log_queries = 1,
         max_block_size = 1000000, group_by_two_level_threshold = 0
FORMAT Null;

SELECT a, b, sum(v) FROM rollup_partitioned GROUP BY a, b WITH ROLLUP
SETTINGS log_comment = '05255-skip-5-sum-int64-independent-partitions', log_queries = 1,
         allow_aggregate_partitions_independently = 1,
         force_aggregate_partitions_independently = 1,
         max_block_size = 100, group_by_two_level_threshold = 1
FORMAT Null;

SELECT v, a, sum(v) FROM rollup_partitioned GROUP BY v, a WITH ROLLUP
SETTINGS log_comment = '05255-skip-6-sum-int64-adaptive', log_queries = 1,
         allow_aggregate_partitions_independently = 0,
         adaptive_aggregator_freeze_threshold = 0,
         max_block_size = 100, group_by_two_level_threshold = 1
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    used_aggregate_functions[1] AS func,
    max(ProfileEvents['AggregationRollupSkippedInputMerges']) AS input_merges_skipped,
    max(ProfileEvents['AggregationConvertedToTwoLevel']) > 0 AS input_was_split,
    max(ProfileEvents['AdaptiveAggregationDrainedRecords']) > 0 AS adaptive_engaged,
    max(ProfileEvents['ExternalAggregationWritePart']) AS spilled
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND query_id = initial_query_id
  AND log_comment LIKE '05255-skip-%'
GROUP BY log_comment, func
ORDER BY log_comment;

-- Run 5 above reports a skipped merge whether or not the partitions were aggregated independently,
-- because either way the input arrives in several blocks. These two read the plan itself, so that
-- the case keeps covering the independently aggregated partitions and not just any split input.

SELECT 'skip merging engaged', replaceRegexpOne(explain, '^.*(Skip merging: [0-9]+).*$', '\1') FROM (
    EXPLAIN actions = 1 SELECT a, b, sum(v) FROM rollup_partitioned GROUP BY a, b WITH ROLLUP
    SETTINGS allow_aggregate_partitions_independently = 1, force_aggregate_partitions_independently = 1
) WHERE explain LIKE '%Skip merging: %';

SELECT 'skip merging disabled', replaceRegexpOne(explain, '^.*(Skip merging: [0-9]+).*$', '\1') FROM (
    EXPLAIN actions = 1 SELECT a, b, sum(v) FROM rollup_partitioned GROUP BY a, b WITH ROLLUP
    SETTINGS allow_aggregate_partitions_independently = 0
) WHERE explain LIKE '%Skip merging: %';

DROP TABLE rollup_partitioned;
DROP TABLE rollup_blocks;
