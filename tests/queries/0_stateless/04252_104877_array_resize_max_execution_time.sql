-- https://github.com/ClickHouse/ClickHouse/issues/104877
-- Regression test for arrayResize honoring max_execution_time when the requested
-- length is very large and the element type is heavy (`Map(Int256, Float64)`).
-- Previously, the inner write loop inside `resizeDynamicSize`/`resizeConstantSize`
-- did not poll the query cancellation flag, so a single call could run for tens of
-- seconds despite a low `max_execution_time` and was reported as `P_TIMEOUT_NOT_HONORED`
-- by the function-properties stress test.
--
-- The poll routes through `QueryStatus::throwIfKilled`, so a `max_execution_time`
-- trip surfaces as `TIMEOUT_EXCEEDED` (matching the rest of query execution),
-- not the generic `QUERY_WAS_CANCELLED`.
SET max_execution_time = 3;

-- Dynamic (`materialize`d) negative size: triggered the original 79s timeout in
-- `function_prop_fuzzer`. With the polling guard in place, this is cancelled within
-- a fraction of the configured budget.
SELECT length(arrayResize([map(toInt256(1), toFloat64(2))], materialize(toInt256(-550164762)))); -- { serverError TIMEOUT_EXCEEDED }

-- Constant negative size: same code path on the `resizeConstantSize` side.
SELECT length(arrayResize([map(toInt256(1), toFloat64(2))], -550164762)); -- { serverError TIMEOUT_EXCEEDED }

-- Constant positive size: covers the extend-with-default branch.
SELECT length(arrayResize([map(toInt256(1), toFloat64(2))], 550164762)); -- { serverError TIMEOUT_EXCEEDED }
