-- Expressions over the right join key must keep its timezone (issue #111033).

SELECT 'hash';
SELECT toString(r.k), r.k, toTypeName(r.k)
FROM (SELECT toDateTime(number, 'UTC') AS k FROM numbers(2)) AS l
INNER JOIN (SELECT toDateTime(number, 'Asia/Tokyo') AS k FROM numbers(2)) AS r ON l.k = r.k
ORDER BY r.k
SETTINGS join_algorithm = 'hash';

SELECT 'parallel_hash';
SELECT toString(r.k), r.k, toTypeName(r.k)
FROM (SELECT toDateTime(number, 'UTC') AS k FROM numbers(2)) AS l
INNER JOIN (SELECT toDateTime(number, 'Asia/Tokyo') AS k FROM numbers(2)) AS r ON l.k = r.k
ORDER BY r.k
SETTINGS join_algorithm = 'parallel_hash';

SELECT 'grace_hash';
SELECT toString(r.k), r.k, toTypeName(r.k)
FROM (SELECT toDateTime(number, 'UTC') AS k FROM numbers(2)) AS l
INNER JOIN (SELECT toDateTime(number, 'Asia/Tokyo') AS k FROM numbers(2)) AS r ON l.k = r.k
ORDER BY r.k
SETTINGS join_algorithm = 'grace_hash';

SELECT 'full_sorting_merge';
SELECT toString(r.k), r.k, toTypeName(r.k)
FROM (SELECT toDateTime(number, 'UTC') AS k FROM numbers(2)) AS l
INNER JOIN (SELECT toDateTime(number, 'Asia/Tokyo') AS k FROM numbers(2)) AS r ON l.k = r.k
ORDER BY r.k
SETTINGS join_algorithm = 'full_sorting_merge';

SELECT 'swapped timezones';
SELECT toString(r.k), r.k, toTypeName(r.k)
FROM (SELECT toDateTime(number, 'Asia/Tokyo') AS k FROM numbers(2)) AS l
INNER JOIN (SELECT toDateTime(number, 'UTC') AS k FROM numbers(2)) AS r ON l.k = r.k
ORDER BY r.k
SETTINGS join_algorithm = 'hash';

SELECT 'DateTime64';
SELECT toString(r.k), r.k, toTypeName(r.k)
FROM (SELECT toDateTime64(number, 3, 'UTC') AS k FROM numbers(2)) AS l
INNER JOIN (SELECT toDateTime64(number, 3, 'Asia/Tokyo') AS k FROM numbers(2)) AS r ON l.k = r.k
ORDER BY r.k
SETTINGS join_algorithm = 'hash';

SELECT 'left join with unmatched rows';
SELECT toString(r.k), r.k, toTypeName(r.k)
FROM (SELECT toDateTime(number, 'UTC') AS k FROM numbers(3)) AS l
LEFT JOIN (SELECT toDateTime(number, 'Asia/Tokyo') AS k FROM numbers(2)) AS r ON l.k = r.k
ORDER BY l.k
SETTINGS join_algorithm = 'hash';
