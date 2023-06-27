SET join_algorithm = 'partial_merge';

SELECT count(1), uniqExact(1) FROM (
SELECT materialize(1) as k FROM numbers(1) nums
JOIN (SELECT materialize(1) AS k, number n FROM numbers(100000)) j
USING k);

SELECT count(1), uniqExact(1) FROM (
SELECT materialize(1) as k FROM numbers(1) nums
JOIN (SELECT 1 AS k, number n FROM numbers(100000)) j
USING k);

SELECT count(1), uniqExact(1) FROM (
SELECT 1 as k FROM numbers(1) nums
JOIN (SELECT materialize(1) AS k, number n FROM numbers(100000)) j
USING k);

SELECT count(1), uniqExact(1) FROM (
SELECT 1 as k FROM numbers(1) nums
JOIN (SELECT 1 AS k, number n FROM numbers(100000)) j
USING k);

SELECT 'first nullable';

SELECT count(1), uniqExact(1) FROM (
SELECT materialize(toNullable(1)) as k FROM numbers(1) nums
JOIN (SELECT materialize(1) AS k, number n FROM numbers(100000)) j
USING k);

SELECT count(1), uniqExact(1) FROM (
SELECT materialize(toNullable(1)) as k FROM numbers(1) nums
JOIN (SELECT 1 AS k, number n FROM numbers(100000)) j
USING k);

SELECT count(1), uniqExact(1) FROM (
SELECT toNullable(1) as k FROM numbers(1) nums
JOIN (SELECT materialize(1) AS k, number n FROM numbers(100000)) j
USING k);

SELECT count(1), uniqExact(1) FROM (
SELECT toNullable(1) as k FROM numbers(1) nums
JOIN (SELECT 1 AS k, number n FROM numbers(100000)) j
USING k);

SELECT 'second nullable';

SELECT count(1), uniqExact(1) FROM (
SELECT materialize(1) as k FROM numbers(1) nums
JOIN (SELECT materialize(toNullable(1)) AS k, number n FROM numbers(100000)) j
USING k);

SELECT count(1), uniqExact(1) FROM (
SELECT materialize(1) as k FROM numbers(1) nums
JOIN (SELECT toNullable(1) AS k, number n FROM numbers(100000)) j
USING k);

SELECT count(1), uniqExact(1) FROM (
SELECT 1 as k FROM numbers(1) nums
JOIN (SELECT materialize(toNullable(1)) AS k, number n FROM numbers(100000)) j
USING k);

SELECT count(1), uniqExact(1) FROM (
SELECT 1 as k FROM numbers(1) nums
JOIN (SELECT toNullable(1) AS k, number n FROM numbers(100000)) j
USING k);

SELECT 'both nullable';

SELECT count(1), uniqExact(1) FROM (
SELECT materialize(toNullable(1)) as k FROM numbers(1) nums
JOIN (SELECT materialize(toNullable(1)) AS k, number n FROM numbers(100000)) j
USING k);

SELECT count(1), uniqExact(1) FROM (
SELECT materialize(toNullable(1)) as k FROM numbers(1) nums
JOIN (SELECT toNullable(1) AS k, number n FROM numbers(100000)) j
USING k);

SELECT count(1), uniqExact(1) FROM (
SELECT toNullable(1) as k FROM numbers(1) nums
JOIN (SELECT materialize(toNullable(1)) AS k, number n FROM numbers(100000)) j
USING k);

SELECT count(1), uniqExact(1) FROM (
SELECT toNullable(1) as k FROM numbers(1) nums
JOIN (SELECT toNullable(1) AS k, number n FROM numbers(100000)) j
USING k);
