SET join_algorithm = 'hash';
SET max_threads = 1, max_block_size = 1024;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET enable_join_runtime_filters = 0, query_plan_join_swap_table = 'false';
SET query_plan_convert_join_to_in = 0, query_plan_propagate_predicate_across_join = 0;

CREATE TABLE rhs (k UInt64) ENGINE = Memory;
INSERT INTO rhs SELECT number FROM numbers(16);

-- The right side is empty only at runtime. Bound reads to catch a missing early stop without a timeout.
SELECT count(), sum(l.k)
FROM (SELECT number AS k FROM system.numbers) AS l
LEFT SEMI JOIN (SELECT k FROM rhs WHERE k >= 16) AS r ON l.k = r.k
SETTINGS max_rows_to_read = 100000;

SELECT count(), sum(l.k)
FROM (SELECT number AS k FROM system.numbers) AS l
INNER JOIN (SELECT k FROM rhs WHERE k >= 16) AS r ON l.k = r.k
SETTINGS max_rows_to_read = 100000;

SELECT count(), sum(l.k)
FROM (SELECT number AS k FROM system.numbers) AS l
RIGHT JOIN (SELECT k FROM rhs WHERE k >= 16) AS r ON l.k = r.k
SETTINGS max_rows_to_read = 100000;

SELECT count(), sum(l.k)
FROM (SELECT number AS k FROM numbers(10)) AS l
LEFT SEMI JOIN (SELECT k FROM rhs WHERE k < 5) AS r ON l.k = r.k;

SELECT count(), sum(l.k)
FROM (SELECT number AS k FROM numbers(10)) AS l
LEFT ANTI JOIN (SELECT k FROM rhs WHERE k >= 16) AS r ON l.k = r.k;

SELECT count(), sum(l.k)
FROM (SELECT number AS k FROM numbers(10)) AS l
LEFT JOIN (SELECT k FROM rhs WHERE k >= 16) AS r ON l.k = r.k;

SELECT count(), sum(l.k)
FROM (SELECT number AS k FROM numbers(10)) AS l
FULL JOIN (SELECT k FROM rhs WHERE k >= 16) AS r ON l.k = r.k;

DROP TABLE rhs;
