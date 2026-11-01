-- Test for issue #75852: window function with large PRECEDING offset
-- that exceeds the number of rows in the partition, combined with small
-- max_block_size that causes early block cleanup.

SELECT
    number,
    p,
    count(*) OVER (PARTITION BY p ORDER BY number DESC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND 65537 PRECEDING)
FROM (
    SELECT number, intDiv(number, 7) AS p
    FROM numbers(71)
)
ORDER BY p ASC NULLS LAST, number DESC NULLS FIRST
SETTINGS max_block_size = 2;
