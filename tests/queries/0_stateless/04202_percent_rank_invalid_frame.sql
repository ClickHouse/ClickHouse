-- Test: exercises `WindowFunctionPercentRank::checkWindowFrameType` rejection branch and
-- the WindowTransform constructor error path that throws BAD_ARGUMENTS.
-- Covers: src/Processors/Transforms/WindowTransform.cpp:2204 (`if (frame != default_window_frame)`)
--         and src/Processors/Transforms/WindowTransform.cpp:397-401 (constructor throw)
--
-- `percent_rank` only supports the default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND
-- UNBOUNDED FOLLOWING. Any other frame must be rejected with BAD_ARGUMENTS at construction
-- time. None of: 03037_precent_rank.sql, 03213_denseRank_percentRank_alias.sql,
-- 03719_ntile_no_partition_by_check.sql, 04136_window_functions_frame_bounds.sql exercise
-- the rejection branch.

-- Wrong frame type: ROWS instead of RANGE.
SELECT x, percent_rank() OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM (SELECT arrayJoin([1, 2, 3, 4]) AS x) ORDER BY x; -- { serverError BAD_ARGUMENTS }

-- Wrong end boundary: CURRENT ROW instead of UNBOUNDED FOLLOWING.
SELECT x, percent_rank() OVER (ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM (SELECT arrayJoin([1, 2, 3, 4]) AS x) ORDER BY x; -- { serverError BAD_ARGUMENTS }

-- Wrong begin boundary: numeric PRECEDING instead of UNBOUNDED.
SELECT x, percent_rank() OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)
FROM (SELECT arrayJoin([1, 2, 3, 4]) AS x) ORDER BY x; -- { serverError BAD_ARGUMENTS }

-- Same rejection applies to the case-insensitive alias `percentRank`.
SELECT x, percentRank() OVER (ORDER BY x ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM (SELECT arrayJoin([1, 2, 3, 4]) AS x) ORDER BY x; -- { serverError BAD_ARGUMENTS }

-- Sanity: the default (no frame specified) and the explicit equivalent are accepted.
SELECT x, percent_rank() OVER (ORDER BY x) FROM (SELECT arrayJoin([1, 2, 3, 4]) AS x) ORDER BY x;
SELECT x, percent_rank() OVER (ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM (SELECT arrayJoin([1, 2, 3, 4]) AS x) ORDER BY x;
