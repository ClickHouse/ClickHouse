-- Tags: no-parallel
-- no-parallel -- enables a process-global failpoint that fires in every -State finalization

-- A combinator that transfers several sub-states in one insertResultInto call aliases them into the
-- result column one by one. When the transfer throws part-way through, the aggregator destroys the
-- whole place while the result column still owns the already-transferred sub-states, so they are
-- freed twice. The failpoint throws from the -State transfer once the destination column already
-- holds an aliased state, which is exactly that partial transfer.
-- Each sub-state below holds more than 4096 bytes of groupArray data, so it is allocated outside the
-- arena and the second destroy reaches a real deallocation.

SYSTEM DISABLE FAILPOINT aggregate_function_state_transfer_throw;

-- Plain -State: the transfer is atomic, so this must throw cleanly on every version.
SYSTEM ENABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT number % 2 AS k, groupArrayState(number) FROM numbers(2000) GROUP BY k ORDER BY k SETTINGS max_threads = 1 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
SYSTEM DISABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT k, length(finalizeAggregation(s)), finalizeAggregation(s)[1] FROM (SELECT number % 2 AS k, groupArrayState(number) AS s FROM numbers(2000) GROUP BY k) ORDER BY k SETTINGS max_threads = 1;

-- -Resample over -ForEach, one element per bucket: the throw is in the second bucket, so -Resample
-- has to undo a completed -ForEach child.
SYSTEM ENABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT groupArrayStateForEachResample(0, 2, 1)([number], number % 2) FROM numbers(2000) SETTINGS max_threads = 1 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
SYSTEM DISABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT arrayMap(x -> arrayMap(y -> length(finalizeAggregation(y)), x), groupArrayStateForEachResample(0, 2, 1)([number], number % 2)) FROM numbers(2000) SETTINGS max_threads = 1;

-- -ForEach alone: the throw is in its own loop, with the first element already aliased.
SYSTEM ENABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT groupArrayStateForEach([number, number + 1]) FROM numbers(2000) SETTINGS max_threads = 1 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
SYSTEM DISABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT arrayMap(x -> length(finalizeAggregation(x)), groupArrayStateForEach([number, number + 1])) FROM numbers(2000) SETTINGS max_threads = 1;

-- Exactly one bucket: the -State transfer cannot throw (the column is still empty), so the throw
-- comes from -Resample appending its own offset with the single sub-state already aliased.
SYSTEM ENABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT groupArrayStateResample(0, 1, 1)(number, 0) FROM numbers(2000) SETTINGS max_threads = 1 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
SYSTEM DISABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT arrayMap(x -> length(finalizeAggregation(x)), groupArrayStateResample(0, 1, 1)(number, 0)) FROM numbers(2000) SETTINGS max_threads = 1;
