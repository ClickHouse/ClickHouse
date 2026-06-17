-- Keys that are bitwise distinct yet compare equal (-0.0 vs +0.0, NaNs with different
-- payloads) hash to different groups but tie under ORDER BY. The heap must never evict
-- a key that ties with the boundary, or a tied group surfaces an incomplete aggregate.

SET enable_group_by_top_k_optimization = 1;
SET max_threads = 1;
SET max_rows_to_group_by = 0;

SELECT 'alternating negative and positive zero, stateful aggregate, LIMIT 1';
SELECT k, uniqExact(v) FROM
(
    SELECT
        if(number % 2 = 0, toFloat64(0), reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9223372036854775808)))) AS k,
        number % 5 AS v
    FROM numbers(100000)
)
GROUP BY k ORDER BY k ASC LIMIT 1;

SELECT 'same result without the optimization';
SELECT k, uniqExact(v) FROM
(
    SELECT
        if(number % 2 = 0, toFloat64(0), reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9223372036854775808)))) AS k,
        number % 5 AS v
    FROM numbers(100000)
)
GROUP BY k ORDER BY k ASC LIMIT 1
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'alternating NaN payloads, LIMIT 1';
SELECT isNaN(k), uniqExact(v) FROM
(
    SELECT
        if(number % 2 = 0,
           reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9221120237041090561))),
           reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9221120237041090562)))) AS k,
        number % 5 AS v
    FROM numbers(100000)
)
GROUP BY k ORDER BY k ASC NULLS FIRST LIMIT 1;

SELECT 'zeros tied at the boundary among real eviction churn';
SELECT count(), countIf(complete) FROM
(
    SELECT l.u = f.u AS complete
    FROM
    (
        SELECT k, uniqExact(v) AS u FROM (SELECT multiIf(number % 4 = 0, toFloat64(0), number % 4 = 1, reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9223372036854775808))), toFloat64(-1000000 + intDiv(toInt64(number), 4))) AS k, number % 5 AS v FROM numbers(100000))
        GROUP BY k ORDER BY k DESC LIMIT 3
        SETTINGS enable_group_by_top_k_optimization = 1
    ) AS l
    INNER JOIN
    (
        SELECT k, uniqExact(v) AS u FROM (SELECT multiIf(number % 4 = 0, toFloat64(0), number % 4 = 1, reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9223372036854775808))), toFloat64(-1000000 + intDiv(toInt64(number), 4))) AS k, number % 5 AS v FROM numbers(100000))
        GROUP BY k
        SETTINGS enable_group_by_top_k_optimization = 0
    ) AS f ON l.k = f.k
);
