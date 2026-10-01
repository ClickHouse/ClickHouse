-- Full sorting has its own final in-memory merge and external spill merge machinery.
-- `max_parallel_ordered_merge_materialization_threads` intentionally does not apply to either.
SET max_threads = 4;
SET max_block_size = 4;
SET max_parallel_ordered_merge_materialization_threads = 4;

SELECT
    countIf(explain LIKE '%MergeSortingTransform%') = 1,
    countIf(explain LIKE '%MergingSortedTransform%') = 1,
    countIf(explain LIKE '%MaterializeMergedDataTransform%') = 0,
    countIf(explain LIKE '%SortChunksBySequenceNumber%') = 0
FROM
(
    EXPLAIN PIPELINE compact = 1
    SELECT number
    FROM numbers_mt(64)
    ORDER BY number
);
