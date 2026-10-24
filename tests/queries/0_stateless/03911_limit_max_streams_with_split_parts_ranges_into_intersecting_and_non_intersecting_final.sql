-- Tags: long, no-sanitizers, no-s3-storage, no-azure-blob-storage
-- long: times out in private
-- no-sanitizers: sometimes times out in private :(
-- no-s3-storage, no-azure-blob-storage: writing 550 small parts to object storage is too slow

DROP TABLE IF EXISTS t;

-- The number of partition (50) as well as parts is much larger than the allowed number of streams. Let's check that we won't produce too many streams:
-- it is still allowed to process each partition independently (thus producing 50 streams), but we should not split parts into smaller ranges within partitions.
-- With that said, since `split_parts_ranges_into_intersecting_and_non_intersecting_final` is enabled, we should also detect non-intersecting ranges, so the
-- resulting number of `AggregatingTransform`-s should be 50 (one per partition) + 5 (for non-intersecting ranges; specific for the given dataset) = 55.
SET split_parts_ranges_into_intersecting_and_non_intersecting_final = 1, split_intersecting_parts_ranges_into_layers_final = 1, do_not_merge_across_partitions_select_final = 1;

CREATE TABLE t
(
    CounterID UInt32,
    UserID UInt64,
    Version UInt64
)
ENGINE = ReplacingMergeTree(Version)
ORDER BY CounterID
PARTITION BY intHash32(UserID) % 50
-- This overrides are needed to fix the pipeline structure
SETTINGS index_granularity = 64, index_granularity_bytes = '1Mi', min_bytes_for_wide_part=10485760, min_rows_for_wide_part=0;

SYSTEM STOP MERGES t;

-- CounterID in [0; 100000)
INSERT INTO t SELECT number, number % 1000, number % 10 FROM numbers_mt(100000);

-- Make around half of the rows to intersect with a smaller parts below

-- CounterID in [0; 5000)
INSERT INTO t SELECT number%5000, number % 1000, number % 10 FROM numbers_mt(5000);
-- CounterID in [10000; 15000)
INSERT INTO t SELECT number%5000+10000, number % 1000, number % 10 FROM numbers_mt(5000);
-- CounterID in [20000; 25000)
INSERT INTO t SELECT number%5000+20000, number % 1000, number % 10 FROM numbers_mt(5000);
-- CounterID in [30000; 35000)
INSERT INTO t SELECT number%5000+30000, number % 1000, number % 10 FROM numbers_mt(5000);
-- CounterID in [40000; 45000)
INSERT INTO t SELECT number%5000+40000, number % 1000, number % 10 FROM numbers_mt(5000);
-- CounterID in [500000; 55000)
INSERT INTO t SELECT number%5000+50000, number % 1000, number % 10 FROM numbers_mt(5000);
-- CounterID in [60000; 65000)
INSERT INTO t SELECT number%5000+60000, number % 1000, number % 10 FROM numbers_mt(5000);
-- CounterID in [70000; 75000)
INSERT INTO t SELECT number%5000+70000, number % 1000, number % 10 FROM numbers_mt(5000);
-- CounterID in [80000; 85000)
INSERT INTO t SELECT number%5000+80000, number % 1000, number % 10 FROM numbers_mt(5000);
-- CounterID in [90000; 95000)
INSERT INTO t SELECT number%5000+90000, number % 1000, number % 10 FROM numbers_mt(5000);

SET max_threads = 32, max_final_threads = 32, max_streams_to_max_threads_ratio = 1;

-- Otherwise there will be no `SelectByIndicesTransform` in the pipeline.
SET enable_vertical_final=1;

SELECT replaceRegexpOne(trimBoth(explain), 'n\d+ (.*)', '\\1') AS explain
FROM (
EXPLAIN PIPELINE graph=1, compact=1
SELECT count() FROM t FINAL GROUP BY UserID
) WHERE explain ILIKE '%SelectByIndicesTransform%' OR explain ILIKE '%AggregatingTransform%'
ORDER BY explain;

DROP TABLE t;
