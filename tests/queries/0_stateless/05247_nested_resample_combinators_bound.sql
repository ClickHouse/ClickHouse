-- Nested -Resample combinators multiply the number of nested states; with an empty innermost range the state size
-- was zero, so the size threshold did not fire, but create/destroy/insert still looped over 2^60 nested states and the
-- query never finished (no cancellation inside). Found by json_ast_sql_execution_fuzzer.
-- The innermost (0, 0, 1) is now rejected directly by the empty-range check in the Resample constructor.
SELECT countResampleIfResampleIfResampleIfResample(0, 0, 1, 0, 1048576, 1, 0, 1048576, 1, 0, 1048576, 1)(number, 1, number, 1, number, 1, number) FROM numbers(1); -- { serverError ARGUMENT_OUT_OF_BOUND }
-- Directly nested -Resample is rejected as an identical combinator, so the levels are interleaved with -If.
-- A nested chain with a nonzero-sized nested state is bounded by `sizeOfData` / `max_state_size`
-- (~32 MiB of count state for a 2048*2048 bucket product is well below the 2^40-byte cap), so it is allowed.
SELECT length(countResampleIfResample(0, 2048, 1, 0, 2048, 1)(number, 1, number)) FROM numbers(1);
SELECT countResampleIfResample(0, 3, 1, 0, 2, 1)(number, 1, number) FROM numbers(10);
SELECT sumResampleIfResampleIfResample(0, 2, 1, 0, 2, 1, 0, 2, 1)(number, number, 1, number, 1, number) FROM numbers(4);
SELECT length(countResample(0, 1048576, 1)(number)) FROM numbers(1);
SELECT countResampleIfResample(0, 0, 1, 0, 1024, 1)(number, 1, number) FROM numbers(1); -- { serverError ARGUMENT_OUT_OF_BOUND }
-- Zero-sized nested state: `sizeOfData` passes vacuously, but every create/destroy/merge loop still iterates over
-- the product of the nested element counts, so the nested-element cap kicks in. `nothing` has a zero-sized state.
SELECT nothingResampleIfResample(0, 2048, 1, 0, 2048, 1)(number, 1, number) FROM numbers(1); -- { serverError ARGUMENT_OUT_OF_BOUND }
