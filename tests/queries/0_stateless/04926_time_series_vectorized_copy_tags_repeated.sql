SELECT '';
SELECT 'timeSeriesCopyTags vectorized repeated dense pairs:';

WITH
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number,
                   timeSeriesTagsToGroup([('dest', toString(number))]) AS group
            FROM numbers(3)
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
           timeSeriesCopyTags(dest_groups[[1, 2, 1, 3, 2, 1][number + 1]], src_groups[[1, 2, 1, 3, 2, 1][number + 1]], ['src']))
FROM numbers(6)
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
            FROM numbers(32)
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
           timeSeriesCopyTags(dest_groups[[1, 25, 1, 25, 1][number + 1]], src_groups[[1, 25, 1, 25, 1][number + 1]], ['src']))
FROM numbers(5)
ORDER BY number;
