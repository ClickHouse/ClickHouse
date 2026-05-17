-- https://github.com/ClickHouse/ClickHouse/issues/104877
-- Regression test for `arrayResize` honoring `max_execution_time` when the
-- requested length is very large and the element type is heavy
-- (`Map(Int256, Float64)`). Previously the inner write loop inside
-- `resizeDynamicSize`/`resizeConstantSize` did not poll the query
-- cancellation flag, so a single call could run for tens of seconds despite
-- a low `max_execution_time` and was reported as `P_TIMEOUT_NOT_HONORED`
-- by `function_prop_fuzzer` (the original reproducer used ~550M elements
-- and ran for ~79 s under `max_execution_time = 3`).
--
-- The poll routes through `QueryStatus::throwIfKilled`, so a
-- `max_execution_time` trip surfaces as `TIMEOUT_EXCEEDED` (matching the
-- rest of query execution). To make this regression deterministic for
-- timeout behaviour we (1) scale the reproducer down to ~200M elements
-- (~1.6 GB of offsets, well under any per-query / OS memory limit on every
-- CI builder including sanitizers, while still keeping the unpatched
-- inner-loop run-time above the 3 s deadline on release builds) and
-- (2) disable the per-query memory limit via `max_memory_usage = 0` so
-- that even with allocator overhead the regression cannot terminate via
-- `MEMORY_LIMIT_EXCEEDED` before the cancellation polling fires. The
-- only way for the assertion below to be satisfied is via the
-- cancellation path we are guarding.
SET max_execution_time = 3;
SET max_memory_usage = 0;

-- Dynamic (`materialize`d) negative size: triggered the original 79 s timeout in
-- `function_prop_fuzzer`. With the polling guard in place, this is cancelled within
-- a fraction of the configured budget.
SELECT length(arrayResize([map(toInt256(1), toFloat64(2))], materialize(toInt256(-200000000)))); -- { serverError TIMEOUT_EXCEEDED }

-- Constant negative size: same code path on the `resizeConstantSize` side.
SELECT length(arrayResize([map(toInt256(1), toFloat64(2))], -200000000)); -- { serverError TIMEOUT_EXCEEDED }

-- Constant positive size: covers the extend-with-default branch.
SELECT length(arrayResize([map(toInt256(1), toFloat64(2))], 200000000)); -- { serverError TIMEOUT_EXCEEDED }
