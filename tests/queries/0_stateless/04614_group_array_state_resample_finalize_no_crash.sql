-- Tags: no-parallel
-- no-parallel -- enables a process-global failpoint that fires in every -State finalization

-- A combinator that transfers several sub-states in one insertResultInto call aliases them into the
-- result column one by one. When the transfer throws part-way through, the aggregator destroys the
-- whole place while the result column still owns the already-transferred sub-states, so they are
-- freed twice. The failpoint throws from the -State transfer once the destination column already
-- holds an aliased state, which is exactly that partial transfer.
-- Each sub-state below holds more than 4096 bytes of groupArray data, so it is allocated outside the
-- arena and the second destroy reaches a real deallocation.

-- The second failpoint throws at the start of a -Tuple element whose predecessor transferred in full,
-- so the child being undone is a COMPLETED one rather than the partially transferred innermost -State.

SYSTEM DISABLE FAILPOINT aggregate_function_state_transfer_throw;
SYSTEM DISABLE FAILPOINT aggregate_function_state_transfer_throw_after_child;

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

-- -Tuple over -Map with String keys: the first element completes, the second throws on its second
-- key, so -Tuple has to undo a completed -Map child in a different subcolumn.
SYSTEM ENABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT groupArrayStateMapTuple((map('a', number), map('b', number, 'c', number))) FROM numbers(2000) SETTINGS max_threads = 1 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
SYSTEM DISABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT mapKeys(t.1), arrayMap(x -> length(finalizeAggregation(x)), mapValues(t.1)), mapKeys(t.2), arrayMap(x -> length(finalizeAggregation(x)), mapValues(t.2)) FROM (SELECT groupArrayStateMapTuple((map('a', number), map('b', number, 'c', number))) AS t FROM numbers(2000)) SETTINGS max_threads = 1;

-- -Tuple over -ForEach, first element shorter than the second.
SYSTEM ENABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT groupArrayStateForEachTuple(([number], [number, number])) FROM numbers(2000) SETTINGS max_threads = 1 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
SYSTEM DISABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT arrayMap(x -> length(finalizeAggregation(x)), t.1), arrayMap(x -> length(finalizeAggregation(x)), t.2) FROM (SELECT groupArrayStateForEachTuple(([number], [number, number])) AS t FROM numbers(2000)) SETTINGS max_threads = 1;

-- Exactly one bucket: the -State transfer cannot throw (the column is still empty), so the throw
-- comes from -Resample appending its own offset with the single sub-state already aliased.
SYSTEM ENABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT groupArrayStateResample(0, 1, 1)(number, 0) FROM numbers(2000) SETTINGS max_threads = 1 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
SYSTEM DISABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT arrayMap(x -> length(finalizeAggregation(x)), groupArrayStateResample(0, 1, 1)(number, 0)) FROM numbers(2000) SETTINGS max_threads = 1;

-- A -Map whose value rows are not uniform: the key with non-NULL values aliases a sub-state, the
-- all-NULL keys make the null adapter insert a default the column itself owns. Undoing such a map in
-- any order other than the reverse of the sorted-key append order applies the wrong row's semantics.
SYSTEM ENABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT groupArrayStateTupleMapTuple((CAST(map('b', NULL, 'a', (number, number + 1), 'c', NULL), 'Map(String, Nullable(Tuple(UInt64, UInt64)))'), CAST(map('x', (number, number + 1), 'y', (number + 2, number + 3)), 'Map(String, Nullable(Tuple(UInt64, UInt64)))'))) FROM numbers(2000) SETTINGS max_threads = 1, enable_nullable_tuple_type = 1 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
SYSTEM DISABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT mapKeys(t.1), arrayMap(x -> isNull(x), mapValues(t.1)), arrayMap(x -> length(finalizeAggregation(assumeNotNull(x).1)), mapValues(t.1)), mapKeys(t.2), arrayMap(x -> isNull(x), mapValues(t.2)) FROM (SELECT groupArrayStateTupleMapTuple((CAST(map('b', NULL, 'a', (number, number + 1), 'c', NULL), 'Map(String, Nullable(Tuple(UInt64, UInt64)))'), CAST(map('x', (number, number + 1), 'y', (number + 2, number + 3)), 'Map(String, Nullable(Tuple(UInt64, UInt64)))'))) AS t FROM numbers(2000)) SETTINGS max_threads = 1, enable_nullable_tuple_type = 1;

-- -Tuple over -Resample.
SYSTEM ENABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT groupArrayStateResampleTuple(0, 2, 1)((number, number + 1), (number % 2, number % 2)) FROM numbers(2000) SETTINGS max_threads = 1 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
SYSTEM DISABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT arrayMap(x -> length(finalizeAggregation(x)), t.1), arrayMap(x -> length(finalizeAggregation(x)), t.2) FROM (SELECT groupArrayStateResampleTuple(0, 2, 1)((number, number + 1), (number % 2, number % 2)) AS t FROM numbers(2000)) SETTINGS max_threads = 1;

-- The same, through a transparent -If wrapper.
SYSTEM ENABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT groupArrayStateResampleIfTuple(0, 2, 1)((number, number + 1), (number % 2, number % 2), (number > 0, number > 0)) FROM numbers(2000) SETTINGS max_threads = 1 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
SYSTEM DISABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT arrayMap(x -> length(finalizeAggregation(x)), t.1), arrayMap(x -> length(finalizeAggregation(x)), t.2) FROM (SELECT groupArrayStateResampleIfTuple(0, 2, 1)((number, number + 1), (number % 2, number % 2), (number > 0, number > 0)) AS t FROM numbers(2000)) SETTINGS max_threads = 1;

-- -OrNull around a Tuple of -State results: Tuple can be inside Nullable, so the transfer goes into
-- the nested column of a ColumnNullable.
SYSTEM ENABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT groupArrayStateResampleTupleOrNullTuple(0, 2, 1)(((number, number + 1), (number + 2, number + 3)), ((number % 2, number % 2), (number % 2, number % 2))) FROM numbers(2000) SETTINGS max_threads = 1 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
SYSTEM DISABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT arrayMap(x -> length(finalizeAggregation(x)), assumeNotNull(t.1).1), arrayMap(x -> length(finalizeAggregation(x)), assumeNotNull(t.2).2) FROM (SELECT groupArrayStateResampleTupleOrNullTuple(0, 2, 1)(((number, number + 1), (number + 2, number + 3)), ((number % 2, number % 2), (number % 2, number % 2))) AS t FROM numbers(2000)) SETTINGS max_threads = 1;

-- The same window through the implicit null adapter instead of -OrNull, plus a -Distinct wrapper.
SYSTEM ENABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT groupArrayStateResampleTupleDistinctTuple(0, 2, 1)(CAST(((number, number + 1), (number + 2, number + 3)), 'Tuple(Nullable(Tuple(UInt64, UInt64)), Nullable(Tuple(UInt64, UInt64)))'), ((number % 2, number % 2), (number % 2, number % 2))) FROM numbers(2000) SETTINGS max_threads = 1, enable_nullable_tuple_type = 1 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
SYSTEM DISABLE FAILPOINT aggregate_function_state_transfer_throw;
SELECT arrayMap(x -> length(finalizeAggregation(x)), assumeNotNull(t.1).1), arrayMap(x -> length(finalizeAggregation(x)), assumeNotNull(t.2).2) FROM (SELECT groupArrayStateResampleTupleDistinctTuple(0, 2, 1)(CAST(((number, number + 1), (number + 2, number + 3)), 'Tuple(Nullable(Tuple(UInt64, UInt64)), Nullable(Tuple(UInt64, UInt64)))'), ((number % 2, number % 2), (number % 2, number % 2))) AS t FROM numbers(2000)) SETTINGS max_threads = 1, enable_nullable_tuple_type = 1;

-- A completed -Resample child undone by its parent: with the second failpoint the first tuple element
-- transfers all of its buckets, and the throw lands before the second element.
SYSTEM ENABLE FAILPOINT aggregate_function_state_transfer_throw_after_child;
SELECT groupArrayStateResampleTuple(0, 2, 1)((number, number + 1), (number % 2, number % 2)) FROM numbers(2000) SETTINGS max_threads = 1 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
SYSTEM DISABLE FAILPOINT aggregate_function_state_transfer_throw_after_child;
SELECT arrayMap(x -> length(finalizeAggregation(x)), t.1), arrayMap(x -> length(finalizeAggregation(x)), t.2) FROM (SELECT groupArrayStateResampleTuple(0, 2, 1)((number, number + 1), (number % 2, number % 2)) AS t FROM numbers(2000)) SETTINGS max_threads = 1;

-- The same completed child behind an -OrNull that forwards: Array cannot be inside Nullable, so -OrFill
-- passes the array column straight through and has to forward the undo as well.
SYSTEM ENABLE FAILPOINT aggregate_function_state_transfer_throw_after_child;
SELECT groupArrayStateResampleOrNullTuple(0, 2, 1)((number, number + 1), (number % 2, number % 2)) FROM numbers(2000) SETTINGS max_threads = 1 FORMAT Null; -- { serverError MEMORY_LIMIT_EXCEEDED }
SYSTEM DISABLE FAILPOINT aggregate_function_state_transfer_throw_after_child;
SELECT arrayMap(x -> length(finalizeAggregation(x)), t.1), arrayMap(x -> length(finalizeAggregation(x)), t.2) FROM (SELECT groupArrayStateResampleOrNullTuple(0, 2, 1)((number, number + 1), (number % 2, number % 2)) AS t FROM numbers(2000)) SETTINGS max_threads = 1;
