SELECT arrayWithConstant(96142475, ['qMUF']); -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT arrayWithConstant(100000000, materialize([[[[[[[[[['Hello, world!']]]]]]]]]])); -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT length(arrayWithConstant(10000000, materialize([[[[[[[[[['Hello world']]]]]]]]]])));

CREATE TABLE args (value Array(Int)) ENGINE=Memory AS SELECT [1, 1, 1, 1] as value FROM numbers(1, 100);
SELECT length(arrayWithConstant(1000000, value)) FROM args FORMAT NULL;
