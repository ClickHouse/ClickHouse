-- Every layer of the `Resample` combinator is checked against the limit on the number of elements
-- on its own, but the sizes of the states multiply, so nested combinators can overflow it.

SELECT countResampleIfResampleIfResampleIfResample(0, 1048576, 1, 0, 1048576, 1, 0, 1048576, 1, 0, 1048576, 1)(number, 1, number, 1, number, 1, number)
FROM numbers(1); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT countResample(0, 4, 1)(number) FROM numbers(4);
SELECT countResampleIfResampleIfResampleIfResample(0, 2, 1, 0, 2, 1, 0, 2, 1, 0, 2, 1)(number, 1, number, 1, number, 1, number) FROM numbers(2);

-- Found by the AST fuzzer: https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=115701&sha=7ba562b7636f1a8219850caaaed32d51ba64519f&name_0=PR&name_1=AST%20fuzzer%20%28amd_debug%2C%20targeted%29
-- Here the product of the sizes does not overflow (it is exactly 2^63 bytes), but such a size
-- is treated as a logical error by the allocator, so it must be cut off by the sanity threshold.
SELECT countResampleIfResampleIfResampleIfResample(0, 1048576, 1, 0, 1, 1, 0, 1048576, 1, 0, 1048576, 1)(number, 1, number, 1, number, 1, number)
FROM numbers(1); -- { serverError ARGUMENT_OUT_OF_BOUND }
