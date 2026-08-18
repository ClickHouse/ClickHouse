SELECT '';
SELECT 'timeSeriesCopyTags vectorized repeated dense pairs:';

WITH
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number,
                   timeSeriesTagsToGroup([('dest', toString(number))]) AS group
            FROM numbers(2)
            ORDER BY number
        )
    ) AS dest_groups,
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number,
                   timeSeriesTagsToGroup([('src', toString(number))]) AS group
            FROM numbers(3)
            ORDER BY number
        )
    ) AS src_groups
SELECT number,
       timeSeriesGroupToTags(
           timeSeriesCopyTags(dest_groups[[1, 2, 1, 1, 2, 1][number + 1]], src_groups[[3, 1, 2, 3, 2, 3][number + 1]], ['src']))
FROM numbers(6)
ORDER BY number;

SELECT '';
SELECT 'timeSeriesCopyTags vectorized near-unique pairs:';

WITH
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number,
                   timeSeriesTagsToGroup([('dest', toString(number))]) AS group
            FROM numbers(10)
            ORDER BY number
        )
    ) AS dest_groups,
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number,
                   timeSeriesTagsToGroup([('src', toString(number))]) AS group
            FROM numbers(10)
            ORDER BY number
        )
    ) AS src_groups
SELECT number,
       timeSeriesGroupToTags(
           timeSeriesCopyTags(dest_groups[[1, 2, 3, 4, 5, 6, 7, 8, 9, 1][number + 1]], src_groups[[10, 9, 8, 7, 6, 5, 4, 3, 2, 10][number + 1]], ['src']))
FROM numbers(10)
ORDER BY number;

SELECT '';
SELECT 'timeSeriesCopyTags vectorized repeated sparse pairs:';

WITH
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number,
                   timeSeriesTagsToGroup([('dest', toString(number))]) AS group
            FROM numbers(2)
            ORDER BY number
        )
    ) AS dest_groups,
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number,
                   timeSeriesTagsToGroup([('src', toString(number))]) AS group
            FROM numbers(32)
            ORDER BY number
        )
    ) AS src_groups
SELECT number,
       timeSeriesGroupToTags(
           timeSeriesCopyTags(dest_groups[[1, 2, 1, 2, 1][number + 1]], src_groups[[25, 1, 25, 1, 25][number + 1]], ['src']))
FROM numbers(5)
ORDER BY number;

SELECT '';
SELECT 'timeSeriesCopyTags vectorized repeated pairs with identical results:';

WITH
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number,
                   timeSeriesTagsToGroup([('dest', toString(number))]) AS group
            FROM numbers(2)
            ORDER BY number
        )
    ) AS dest_groups,
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT timeSeriesTagsToGroup([('src', 'same')]) AS group
        )
    ) AS src_groups
SELECT count(),
       uniqExact(new_group),
       groupUniqArray(timeSeriesGroupToTags(new_group))
FROM
(
    SELECT timeSeriesCopyTags(dest_groups[[1, 2, 1][number + 1]], src_groups[1], ['dest']) AS new_group
    FROM numbers(3)
);
