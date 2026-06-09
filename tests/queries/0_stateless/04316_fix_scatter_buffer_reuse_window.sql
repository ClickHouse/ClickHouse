-- Test that ScatterByPartitionTransform correctly re-initializes its hash buffer
-- across multiple blocks, so a later block that is <= a prior block's size never
-- reuses stale hash values from the previous block.
SELECT count() AS bad_rows
FROM
(
    SELECT number % 977 AS k, sum(number) OVER (PARTITION BY number % 977) AS w
    FROM numbers(2000000)
    SETTINGS max_threads = 8
) a
INNER JOIN (
    SELECT number % 977 AS k, sum(number) AS g
    FROM numbers(2000000)
    GROUP BY k
) b USING (k)
WHERE w != g;
