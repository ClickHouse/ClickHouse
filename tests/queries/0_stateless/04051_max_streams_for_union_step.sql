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

-- 4. Ratio = 0 disables ratio-based limit, only max_streams_for_union_step = 3 applies.
-- Uses 6 branches so the 6 → 3 narrowing produces an even 2,2,2 split — the EXPLAIN
-- PIPELINE output groups Concat processors by name only, so an uneven split (e.g. 4 → 3)
-- would render with the IO ports of an arbitrary Concat in the group, which is unstable
-- because `narrowPipe` shuffles its distribution.
EXPLAIN PIPELINE
SELECT 1
UNION ALL
SELECT 2
UNION ALL
SELECT 3
UNION ALL
SELECT 4
UNION ALL
SELECT 5
UNION ALL
SELECT 6
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

-- 8. Small positive ratio (0.5 * 1 thread = 0.5) must clamp to 1, not silently disable
EXPLAIN PIPELINE
SELECT 1
UNION ALL
SELECT 2
UNION ALL
SELECT 3
UNION ALL
SELECT 4
SETTINGS max_threads = 1, max_streams_for_union_step = 0, max_streams_for_union_step_to_max_threads_ratio = 0.5;

-- 9. Negative ratio must be rejected by `UnionStep::updatePipeline`
SELECT 1 UNION ALL SELECT 2
SETTINGS max_streams_for_union_step_to_max_threads_ratio = -1; -- { serverError PARAMETER_OUT_OF_BOUND }

-- 10. Non-finite ratio (infinity) is rejected by the Float setting parser before
-- our validation runs. The `isFinite` guard in `UnionStep::updatePipeline` is
-- still useful as defense in depth for distributed query serialization paths.
SELECT 1 UNION ALL SELECT 2
SETTINGS max_streams_for_union_step_to_max_threads_ratio = inf; -- { clientError CANNOT_PARSE_NUMBER }

-- 11. Non-finite ratio (NaN) is also rejected by the Float setting parser.
SELECT 1 UNION ALL SELECT 2
SETTINGS max_streams_for_union_step_to_max_threads_ratio = nan; -- { clientError CANNOT_PARSE_NUMBER }

-- 12. UNION DISTINCT goes through `UnionStep` followed by a DistinctStep,
-- so the narrowing must apply to the union pipeline and not break correctness.
SELECT count() FROM
(
    SELECT 1 AS x
    UNION DISTINCT SELECT 2
    UNION DISTINCT SELECT 1
    UNION DISTINCT SELECT 3
    UNION DISTINCT SELECT 2
)
SETTINGS max_streams_for_union_step = 2, max_streams_for_union_step_to_max_threads_ratio = 0;

SELECT sum(x) FROM
(
    SELECT 1 AS x
    UNION DISTINCT SELECT 2
    UNION DISTINCT SELECT 1
    UNION DISTINCT SELECT 3
    UNION DISTINCT SELECT 2
)
SETTINGS max_threads = 4, max_streams_for_union_step = 0, max_streams_for_union_step_to_max_threads_ratio = 1;
