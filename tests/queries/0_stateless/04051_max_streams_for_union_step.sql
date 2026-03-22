-- Test that max_streams_for_union_step and max_streams_for_union_step_to_max_threads_ratio
-- limit the number of active streams in UNION ALL.

-- 1. Explicit max_streams_for_union_step = 2 produces Concat processors
EXPLAIN PIPELINE
SELECT 1
UNION ALL
SELECT 2
UNION ALL
SELECT 3
UNION ALL
SELECT 4
SETTINGS max_threads = 4, max_streams_for_union_step = 2, max_streams_for_union_step_to_max_threads_ratio = 0;

-- 2. Ratio = 1, max_threads = 2 narrows 4 branches to 2 streams
EXPLAIN PIPELINE
SELECT 1
UNION ALL
SELECT 2
UNION ALL
SELECT 3
UNION ALL
SELECT 4
SETTINGS max_threads = 2, max_streams_for_union_step_to_max_threads_ratio = 1;

-- 3. Both settings active: min(max_streams_for_union_step=3, max_threads*ratio=2) = 2
EXPLAIN PIPELINE
SELECT 1
UNION ALL
SELECT 2
UNION ALL
SELECT 3
UNION ALL
SELECT 4
SETTINGS max_threads = 2, max_streams_for_union_step = 3, max_streams_for_union_step_to_max_threads_ratio = 1;

-- 4. Ratio = 0 disables ratio-based limit, only max_streams_for_union_step = 3 applies
EXPLAIN PIPELINE
SELECT 1
UNION ALL
SELECT 2
UNION ALL
SELECT 3
UNION ALL
SELECT 4
SETTINGS max_threads = 2, max_streams_for_union_step = 3, max_streams_for_union_step_to_max_threads_ratio = 0;

-- 5. Both settings 0: no narrowing at all
EXPLAIN PIPELINE
SELECT 1
UNION ALL
SELECT 2
UNION ALL
SELECT 3
UNION ALL
SELECT 4
SETTINGS max_threads = 2, max_streams_for_union_step = 0, max_streams_for_union_step_to_max_threads_ratio = 0;

-- 6. Verify results are correct with narrowing
SELECT sum(x) FROM
(
    SELECT 1 AS x
    UNION ALL SELECT 2
    UNION ALL SELECT 3
    UNION ALL SELECT 4
    UNION ALL SELECT 5
)
SETTINGS max_streams_for_union_step = 2, max_streams_for_union_step_to_max_threads_ratio = 0;

-- 7. Verify results are correct without narrowing
SELECT sum(x) FROM
(
    SELECT 1 AS x
    UNION ALL SELECT 2
    UNION ALL SELECT 3
    UNION ALL SELECT 4
    UNION ALL SELECT 5
)
SETTINGS max_streams_for_union_step = 0, max_streams_for_union_step_to_max_threads_ratio = 0;
