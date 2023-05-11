SET max_memory_usage = 50000000;
SET join_algorithm = 'partial_merge';

SELECT 'defaults';

SELECT count(ignore(*)) FROM (
    SELECT k, n FROM (SELECT materialize(1) as k FROM numbers(10)) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(1000000)) j
    USING k);

SELECT count(ignore(*)) FROM (
    SELECT k, n FROM (SELECT materialize(1) as k FROM numbers(1000)) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(10000)) j
    USING k);

SELECT count(ignore(*)), uniqExact(n) FROM (
    SELECT k, n FROM (SELECT materialize(1) as k FROM numbers(1000000)) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(10)) j
    USING k);

-- errors
SET max_joined_block_size_rows = 0;

SELECT count(ignore(*)) FROM (
    SELECT k, n FROM (SELECT materialize(1) as k FROM numbers(10)) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(1000000)) j
    USING k); -- { serverError 241 }

SELECT count(ignore(*)) FROM (
    SELECT k, n FROM (SELECT materialize(1) as k FROM numbers(1000)) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(10000)) j
    USING k); -- { serverError 241 }

SELECT 'max_joined_block_size_rows = 2000';
SET max_joined_block_size_rows = 2000;

SELECT count(ignore(*)) FROM (
    SELECT k, n FROM (SELECT materialize(1) as k FROM numbers(10)) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(1000000)) j
    USING k);

SELECT count(ignore(*)), uniqExact(n) FROM (
    SELECT k, n FROM (SELECT materialize(1) as k FROM numbers(1000)) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(10000)) j
    USING k);

SELECT count(ignore(*)), uniqExact(n) FROM (
    SELECT k, n FROM (SELECT materialize(1) as k FROM numbers(1000000)) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(10)) j
    USING k);

SELECT 'max_rows_in_join = 1000';
SET max_rows_in_join = 1000;

SELECT count(ignore(*)) FROM (
    SELECT k, n FROM (SELECT materialize(1) as k FROM numbers(10)) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(1000000)) j
    USING k);

SELECT count(ignore(*)), uniqExact(n) FROM (
    SELECT k, n FROM (SELECT materialize(1) as k FROM numbers(1000)) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(10000)) j
    USING k);

SELECT count(ignore(*)), uniqExact(n) FROM (
    SELECT k, n FROM (SELECT materialize(1) as k FROM numbers(1000000)) nums
    JOIN (SELECT materialize(1) AS k, number n FROM numbers(10)) j
    USING k);
