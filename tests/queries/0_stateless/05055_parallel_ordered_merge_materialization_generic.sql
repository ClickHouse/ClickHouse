-- Exercise the generic `FinishSorting` path rather than a MergeTree in-order read. The `LIMIT` makes
-- each branch's order semantically observable, and `UNION ALL` exposes the branches as separate streams
-- with a common sort prefix. `applyOrder` can then convert the outer sort to a multi-stream `FinishSorting`.
SET optimize_read_in_order = 0;
SET optimize_sorting_by_input_stream_properties = 1;
SET max_threads = 4;
SET max_parallel_ordered_merge_materialization_threads = 4;

SELECT
    countIf(explain LIKE '%MaterializeMergedDataTransform%') = 1,
    countIf(explain LIKE '%SortChunksBySequenceNumber%') = 1,
    countIf(explain LIKE '%FinishSortingTransform%') = 1
FROM
(
    EXPLAIN PIPELINE compact = 1
    SELECT *
    FROM
    (
        SELECT *
        FROM
        (
            SELECT number % 4 AS prefix, number AS value
            FROM numbers_mt(64)
            ORDER BY prefix
            LIMIT 64
        )
        UNION ALL
        SELECT *
        FROM
        (
            SELECT number % 4 AS prefix, number + 64 AS value
            FROM numbers_mt(64)
            ORDER BY prefix
            LIMIT 64
        )
    )
    ORDER BY prefix, value
);
