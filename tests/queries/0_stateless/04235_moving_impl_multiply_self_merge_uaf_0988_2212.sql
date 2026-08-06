-- Tags: no-fasttest
-- ^ The reproducer surfaces as a `Segmentation fault` / `use-after-poison`
--   on AddressSanitizer / MemorySanitizer builds, and as a PODArray
--   `insertPrepare` assertion (`assertNotIntersects`) on Debug builds.
--   On Release builds the corruption can be silent, so the no-fasttest tag
--   restricts the test to builds where the regression is observable.

-- Regression test for STID 0988-2212: `Segmentation fault` inside
-- `DB::MovingImpl::merge` (`AggregateFunctionGroupArrayMoving`) reached via
-- `FunctionBinaryArithmetic<MultiplyImpl>::executeAggregateMultiply` during
-- the analyzer dry-run. Same bug class as STID 0988-40af / 0988-3351 — the
-- exponentiation-by-squaring loop in `executeAggregateMultiply` invoked
-- `function->merge(state, state)`, aliasing the source and destination
-- aggregate state. `MovingImpl::merge` then called
-- `cur_elems.value.insert(rhs_elems.value.begin(), rhs_elems.value.end(), arena)`
-- where `rhs_elems` referenced the same `PODArray` as `cur_elems.value`, so
-- the source iterators pointed into the buffer that `insertPrepare` was
-- about to reallocate. The fix removes the self-alias at the call site by
-- copying `vec_from` into an independent column before each squaring step.

-- Length grows linearly with the multiplier: each squaring concatenates the
-- backing prefix-sum array. For 50 input rows, `state * N` materialises
-- `N * 50` prefix sums.
SELECT length(finalizeAggregation(state * 1))  FROM (SELECT groupArrayMovingSumState(number) AS state FROM numbers(50));
SELECT length(finalizeAggregation(state * 2))  FROM (SELECT groupArrayMovingSumState(number) AS state FROM numbers(50));
SELECT length(finalizeAggregation(state * 3))  FROM (SELECT groupArrayMovingSumState(number) AS state FROM numbers(50));
SELECT length(finalizeAggregation(state * 4))  FROM (SELECT groupArrayMovingSumState(number) AS state FROM numbers(50));
SELECT length(finalizeAggregation(state * 8))  FROM (SELECT groupArrayMovingSumState(number) AS state FROM numbers(50));
SELECT length(finalizeAggregation(state * 16)) FROM (SELECT groupArrayMovingSumState(number) AS state FROM numbers(50));

-- groupArrayMovingAvg exercises the same `MovingImpl::merge` code path with
-- a different `Data::Accumulator` (Float64 / Decimal).
SELECT length(finalizeAggregation(state * 4)) FROM (SELECT groupArrayMovingAvgState(number) AS state FROM numbers(50));

-- Decimal accumulator path: `MovingImpl<..., MovingSumData<Decimal<...>>>::merge`
-- — the variant caught on PR #101062 (STID 0988-2212 on amd_debug). The
-- squaring branch exercises the same self-alias hazard.
SELECT length(finalizeAggregation(state * 4)) FROM (SELECT groupArrayMovingSumState(toDecimal64(number, 2)) AS state FROM numbers(50));

-- Windowed variant `groupArrayMovingSum(window_size)`: parameterised template
-- (`LimitNumElements::value = true`) exercises the same merge path, with a
-- different `MovingImpl` instantiation.
SELECT length(finalizeAggregation(state * 4)) FROM (SELECT groupArrayMovingSumState(5)(number) AS state FROM numbers(50));

-- Constant column path: `executeAggregateMultiply` takes the `size = 1`
-- branch through `agg_state_is_const` and produces a `ColumnConst`.
SELECT length(finalizeAggregation(arrayJoin([state, state]) * 4))
FROM (SELECT groupArrayMovingSumState(number) AS state FROM numbers(50));
