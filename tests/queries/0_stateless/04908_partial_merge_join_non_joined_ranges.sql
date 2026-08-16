-- The right side is split into two blocks. The left keys create unmatched runs of
-- different lengths in each block: [0], [3, 4], [7], [8, 9], and [11, 15].
SET join_algorithm = 'partial_merge', partial_merge_join_rows_in_right_blocks = 8, max_block_size = 8, max_threads = 1, join_use_nulls = 1;

SELECT
    count() = 16,
    sum(r.k) = 120,
    sum(l.value) = 524,
    countIf(isNull(r.payload)) = 4,
    countIf(isNull(r.text)) = 0
FROM values('k UInt64, value UInt64', (1, 101), (2, 102), (5, 105), (6, 106), (10, 110)) AS l
ALL RIGHT JOIN
(
    SELECT
        number AS k,
        CAST(if(number % 5 = 0, NULL, number + 1000) AS Nullable(UInt64)) AS payload,
        toString(number) AS text
    FROM numbers(16)
) AS r ON l.k = r.k;

SELECT
    count() = 17,
    sum(r.k) = 120,
    sum(l.value) = 724,
    countIf(isNull(l.value)) = 11,
    countIf(isNull(r.payload)) = 5,
    countIf(isNull(r.text)) = 1
FROM values('k UInt64, value UInt64', (1, 101), (2, 102), (5, 105), (6, 106), (10, 110), (100, 200)) AS l
ALL FULL JOIN
(
    SELECT
        number AS k,
        CAST(if(number % 5 = 0, NULL, number + 1000) AS Nullable(UInt64)) AS payload,
        toString(number) AS text
    FROM numbers(16)
) AS r ON l.k = r.k;
