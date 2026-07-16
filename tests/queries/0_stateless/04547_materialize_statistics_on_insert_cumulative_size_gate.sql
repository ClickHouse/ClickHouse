-- Tags: no-random-settings
-- no-random-settings: the size gate below depends on precise block byte sizes; settings
-- randomization (e.g. of max_block_size) could change how the INSERT is chunked and split
-- across partitions, making the "some parts get statistics, some don't" split flaky.

-- Regression test: `materialize_statistics_on_insert_max_table_size` must bound statistics
-- materialization cumulatively across a whole INSERT, not just per part. `MergeTreeSink::consume`
-- (and `ReplicatedMergeTreeSink`) write all temporary parts of one INSERT before any of them
-- become active, so a single check against `getTotalActiveSizeInBytes()` alone would see ~0
-- active bytes for every part of a highly-partitioned bulk load and materialize statistics for
-- all of them regardless of their combined size.

SET allow_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET materialize_statistics_on_insert_max_table_size = 250000;

DROP TABLE IF EXISTS t_stats_cumulative_insert;
CREATE TABLE t_stats_cumulative_insert
(
    p UInt64,
    v UInt64 STATISTICS(minmax)
)
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY p
SETTINGS auto_statistics_types = '';

SYSTEM STOP MERGES t_stats_cumulative_insert;

-- A single INSERT split into 3 partitions of 10000 rows each (~160000 bytes of raw block data
-- per partition: 2 UInt64 columns * 8 bytes * 10000 rows). Each partition alone is well under
-- the 250000-byte cap, but the second and third partitions push the cumulative size (of parts
-- already written earlier in this same INSERT, not yet active) past it.
INSERT INTO t_stats_cumulative_insert SELECT number % 3 AS p, number AS v FROM numbers(30000);

SELECT count() FROM t_stats_cumulative_insert;

-- Exactly one of the three parts (whichever the writer processed first) must have materialized
-- statistics; the other two must have none, because they push the cumulative size of the INSERT
-- past `materialize_statistics_on_insert_max_table_size`.
SELECT countIf(notEmpty(statistics)) AS parts_with_statistics, countIf(empty(statistics)) AS parts_without_statistics
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stats_cumulative_insert' AND active AND column = 'v';

DROP TABLE t_stats_cumulative_insert;
