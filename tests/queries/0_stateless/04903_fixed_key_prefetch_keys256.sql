SELECT count() > 0 FROM
(
    EXPLAIN SELECT k, count()
    FROM
    (
        SELECT toUInt256(number % 3) AS k
        FROM numbers(100000)
    )
    GROUP BY k
)
WHERE explain ILIKE '%Keys: toUInt256%';

SELECT k, count()
FROM
(
    SELECT toUInt256(number % 3) AS k
    FROM numbers(100000)
)
GROUP BY k
ORDER BY k
SETTINGS enable_software_prefetch_in_aggregation = 1;
