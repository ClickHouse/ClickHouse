-- A query that runs out of time stops inside arrayResize and before evaluating its next constant expression.
-- A query that keeps evaluating after its deadline reports the division by zero instead.

SELECT intDiv(length(arrayResize([], 500000000)), 0) SETTINGS max_execution_time = 0.1, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }

-- A function call has no partial result, so it fails in the break mode too, extending to the right and to the left.
SELECT intDiv(length(arrayResize([], 500000000)), 0) SETTINGS max_execution_time = 0.1, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }
SELECT intDiv(length(arrayResize([], -500000000)), 0) SETTINGS max_execution_time = 0.1, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }
-- Copies of a large extender are counted by their size, so a few thousand of them stop at the deadline too.
SELECT intDiv(length(arrayResize([], 8000, repeat('x', 16384))), 0) SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }

SELECT length(arrayWithConstant(50000000, [])), intDiv(1, 0) SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }
SELECT intDiv(length(arrayWithConstant(50000000, [])), 0) SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }
