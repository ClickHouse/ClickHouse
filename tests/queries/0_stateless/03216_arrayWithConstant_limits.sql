SELECT arrayWithConstant(96142475, ['qMUF']); -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT arrayWithConstant(100000000, materialize([[[[[[[[[['Hello, world!']]]]]]]]]])); -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT length(arrayWithConstant(10000000, materialize([[[[[[[[[['Hello world']]]]]]]]]])));

CREATE TABLE args (value Array(Int)) ENGINE=Memory;
-- If all rows form a single block, the query below builds a single huge column, which does
-- not fit into the memory limit on a loaded test runner. `Memory` returns stored blocks as-is
-- (`max_block_size` cannot split them on read), so store small blocks at insert time instead.
-- Small blocks exercise the same code path with a much smaller peak allocation, and 10 rows
-- instead of 100 keep the test under the time limit with sanitizers.
INSERT INTO args SELECT [1, 1, 1, 1] FROM numbers(1, 10) SETTINGS max_block_size = 2, min_insert_block_size_rows = 2, min_insert_block_size_bytes = 1;
SELECT length(arrayWithConstant(1000000, value)) FROM args FORMAT NULL SETTINGS max_threads = 1;
