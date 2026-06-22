-- Tags: no-fasttest, no-random-settings, no-parallel-replicas

-- arrayFold folds an entire array inside a single executeImpl() call, so the pipeline-level time check
-- (which only runs between blocks) cannot interrupt a fold over one very long array. Each query below
-- folds a single 400000-element array in one executeImpl(), so only the in-function check added by this
-- PR can stop it. Without that check the fold runs uninterruptibly and ignores the time limit.

-- throw mode: the in-function check sees the limit and throws promptly.
SELECT ignore(arrayFold((acc, x) -> acc + cityHash64(toString(x)), range(400000), toUInt64(0)))
SETTINGS max_execution_time = 1, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }

-- break mode: checkTimeLimit() returns false instead of throwing. A fold has no meaningful partial
-- result, so the in-function check turns that false into a hard stop. This is the path the code ignored
-- before this PR: it discarded the false return and kept folding to completion.
SELECT ignore(arrayFold((acc, x) -> acc + cityHash64(toString(x)), range(400000), toUInt64(0)))
SETTINGS max_execution_time = 1, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }
