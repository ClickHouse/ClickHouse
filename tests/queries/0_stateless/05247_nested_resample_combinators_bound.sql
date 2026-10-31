-- Nested -Resample combinators multiply the number of nested states; with an empty innermost range the state size
-- was zero, so the size threshold did not fire, but create/destroy/insert still looped over 2^60 nested states and the
-- query never finished (no cancellation inside). Found by json_ast_sql_execution_fuzzer.
SELECT countResampleIfResampleIfResampleIfResample(0, 0, 1, 0, 1048576, 1, 0, 1048576, 1, 0, 1048576, 1)(number, 1, number, 1, number, 1, number) FROM numbers(1); -- { serverError ARGUMENT_OUT_OF_BOUND }
-- Directly nested -Resample is rejected as an identical combinator, so the levels are interleaved with -If.
SELECT countResampleIfResample(0, 2048, 1, 0, 2048, 1)(number, 1, number) FROM numbers(1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT countResampleIfResample(0, 3, 1, 0, 2, 1)(number, 1, number) FROM numbers(10);
SELECT sumResampleIfResampleIfResample(0, 2, 1, 0, 2, 1, 0, 2, 1)(number, number, 1, number, 1, number) FROM numbers(4);
SELECT length(countResample(0, 1048576, 1)(number)) FROM numbers(1);
SELECT countResampleIfResample(0, 0, 1, 0, 1024, 1)(number, 1, number) FROM numbers(1); -- { serverError ARGUMENT_OUT_OF_BOUND }
