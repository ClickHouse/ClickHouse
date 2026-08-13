SET max_memory_usage = 12000000;
SET join_algorithm = 'partial_merge';
SET analyzer_compatibility_join_using_top_level_identifier = 1;
SET joined_block_split_single_row = 0;
SET max_block_size = 8192; -- CI may inject large block sizes; partial_merge join allocates blocks proportional to max_block_size, which can exceed the 12 MB memory limit above
SET enable_join_runtime_filters = 0; -- CI may inject True; runtime filter bloom structures add memory overhead pushing partial_merge join over the 12 MB limit

SELECT 'defaults';

SELECT count(1) FROM (
    SELECT materialize(1) as k, n FROM numbers(10) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(100000)) j
    USING k);

SELECT count(1) FROM (
    SELECT materialize(1) as k, n FROM numbers(100) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(10000)) j
    USING k);

SELECT count(1), uniqExact(n) FROM (
    SELECT materialize(1) as k, n FROM numbers(100000) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(10)) j
    USING k);

SET max_joined_block_size_rows = 0;

SET query_plan_join_swap_table = 'false';

-- Because of the optimizations in the analyzer the following queries started to run without issues. To keep the essence of the test, we test both cases.
SELECT count(1) FROM (
    SELECT materialize(1) as k, n FROM numbers(10) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(100000)) j
    USING k) SETTINGS enable_analyzer = 0; -- { serverError MEMORY_LIMIT_EXCEEDED }

SELECT count(1) FROM (
    SELECT materialize(1) as k, n FROM numbers(100) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(10000)) j
    USING k) SETTINGS enable_analyzer = 0; -- { serverError MEMORY_LIMIT_EXCEEDED }

SELECT count(1) FROM (
    SELECT materialize(1) as k, n FROM numbers(10) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(100000)) j
    USING k) SETTINGS enable_analyzer = 1, max_block_size = 8192, max_joined_block_size_rows = 0, enable_join_runtime_filters = 0, max_threads = 1, query_plan_remove_unused_columns = 1; -- CI may inject enable_join_runtime_filters=True (bloom overhead), max_threads>1 (each thread holds separate sort-merge buffers), or query_plan_remove_unused_columns=False (join keeps all columns in sort-merge buffer); with max_joined_block_size_rows=0 any of these pushes partial_merge join over the 12 MB limit
SELECT count(1) FROM (
    SELECT materialize(1) as k, n FROM numbers(100) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(10000)) j
    USING k) SETTINGS enable_analyzer = 1, max_block_size = 8192, max_joined_block_size_rows = 0, enable_join_runtime_filters = 0, max_threads = 1, query_plan_remove_unused_columns = 1; -- CI may inject enable_join_runtime_filters=True (bloom overhead), max_threads>1 (each thread holds separate sort-merge buffers), or query_plan_remove_unused_columns=False (join keeps all columns in sort-merge buffer); with max_joined_block_size_rows=0 any of these pushes partial_merge join over the 12 MB limit

SELECT 'max_joined_block_size_rows = 2000';
SET max_joined_block_size_rows = 2000;

SELECT count(1) FROM (
    SELECT materialize(1) as k, n FROM numbers(10) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(100000)) j
    USING k);

SELECT count(1), uniqExact(n) FROM (
    SELECT materialize(1) as k, n FROM numbers(100) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(10000)) j
    USING k);

SELECT count(1), uniqExact(n) FROM (
    SELECT materialize(1) as k, n FROM numbers(100000) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(10)) j
    USING k);

SELECT 'max_rows_in_join = 1000';
SET max_rows_in_join = 1000;

SELECT count(1) FROM (
    SELECT materialize(1) as k, n FROM numbers(10) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(100000)) j
    USING k);

SELECT count(1), uniqExact(n) FROM (
    SELECT materialize(1) as k, n FROM numbers(100) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(10000)) j
    USING k);

SELECT count(1), uniqExact(n) FROM (
    SELECT materialize(1) as k, n FROM numbers(100000) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(10)) j
    USING k);
