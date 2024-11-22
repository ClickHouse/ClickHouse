SELECT arrayWithConstant(96142475, ['qMUF']); -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT arrayWithConstant(100000000, materialize([[[[[[[[[['Hello, world!']]]]]]]]]])); -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT length(arrayWithConstant(10000000, materialize([[[[[[[[[['Hello world']]]]]]]]]])));
