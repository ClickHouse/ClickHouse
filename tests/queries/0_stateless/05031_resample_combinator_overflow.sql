-- Every layer of the `Resample` combinator is checked against the limit on the number of elements
-- on its own, but the sizes of the states multiply, so nested combinators can overflow it.

SELECT countResampleIfResampleIfResampleIfResample(0, 1048576, 1, 0, 1048576, 1, 0, 1048576, 1, 0, 1048576, 1)(number, 1, number, 1, number, 1, number)
FROM numbers(1); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT countResample(0, 4, 1)(number) FROM numbers(4);
SELECT countResampleIfResampleIfResampleIfResample(0, 2, 1, 0, 2, 1, 0, 2, 1, 0, 2, 1)(number, 1, number, 1, number, 1, number) FROM numbers(2);
