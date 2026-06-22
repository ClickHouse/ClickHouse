-- Tags: no-fasttest, no-random-settings, no-parallel-replicas

-- arrayFold folds an entire chunk inside a single executeImpl() call, so the pipeline-level
-- time/cancellation check (which only runs between chunks) cannot interrupt a fold over a very
-- long array. Before the in-loop check was added, this query ignored max_execution_time and ran
-- to completion (observed in CI as a ~2700s uncancellable hang). It must now time out promptly.
SELECT sum(s)
FROM
(
    SELECT arrayFold((acc, x) -> acc + x, arr, toUInt64(0)) AS s
    FROM (SELECT range(20000) AS arr FROM numbers(20000))
)
SETTINGS max_execution_time = 1, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }
