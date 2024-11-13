SELECT arrayWithConstant(96142475, ['qMUF']); -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT arrayWithConstant(100000000, materialize([[[[[[[[[['Hello, world!']]]]]]]]]])); -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT length(arrayWithConstant(10000000, materialize([[[[[[[[[['Hello world']]]]]]]]]])));

CREATE TEMPORARY TABLE args (value Array(Int)) ENGINE=Memory AS SELECT [1, 1, 1, 1] as value FROM numbers(1, 10000);
CREATE TEMPORARY TABLE results (value Array(Array(Int))) ENGINE=Memory AS SELECT arrayWithConstant(10000, value) FROM args;
