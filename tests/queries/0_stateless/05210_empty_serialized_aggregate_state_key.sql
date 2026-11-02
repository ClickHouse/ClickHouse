-- An aggregate function state must never serialize to zero bytes: a column of states is read back one
-- state at a time, with no length in front of any of them. Two parameterisations held nothing, so they
-- are rejected where the function is created.

-- An empty Resample range, both ways of writing one.
SELECT countResample(10, 5, 1)(number, number) FROM numbers(10); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT countResample(5, 5, 1)(number, number) FROM numbers(10); -- { serverError ARGUMENT_OUT_OF_BOUND }
-- The state type is rejected too, so such a column cannot be declared.
SELECT CAST('' AS AggregateFunction(countResample(10, 5, 1), UInt64, UInt64)); -- { serverError ARGUMENT_OUT_OF_BOUND }
-- Controls: a non-empty range keeps working, including the smallest one.
SELECT countResample(1, 5, 1)(number, number) FROM numbers(10);
SELECT countResample(4, 5, 1)(number, number) FROM numbers(10);

-- A matrix aggregate function needs at least one argument; all three kinds share one creator.
SELECT covarSampMatrix() FROM numbers(3); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
-- Control: one argument keeps working.
SELECT length(covarSampMatrix(number)) FROM numbers(3);
