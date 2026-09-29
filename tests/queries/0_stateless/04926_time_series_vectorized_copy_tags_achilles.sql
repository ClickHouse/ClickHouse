SELECT 'single vectorized pair';
SELECT timeSeriesGroupToTags(timeSeriesCopyTags(
    materialize(timeSeriesTagsToGroup([('dest', 'a')])),
    materialize(timeSeriesTagsToGroup([('src', 'b')])), ['src']));

SELECT 'pair cardinality and shuffled row order';
WITH
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number, timeSeriesTagsToGroup([('dest', toString(number))]) AS group
            FROM numbers(100)
            ORDER BY number
        )
    ) AS dest_groups,
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number, timeSeriesTagsToGroup([('src', toString(number))]) AS group
            FROM numbers(100)
            ORDER BY number
        )
    ) AS src_groups,
    [1, 10, 15, 20, 30, 50, 75, 80, 89, 90, 91, 95, 99, 100] AS cutoffs,
    cutoffs[intDiv(number, 100) + 1] AS unique_pairs,
    (number * 37 + 11) % 100 AS row_index,
    if(row_index < unique_pairs, row_index, 0) AS pair_index,
    timeSeriesCopyTags(dest_groups[pair_index + 1], src_groups[100 - pair_index], ['src']) AS result
SELECT unique_pairs, count(), uniqExact(result),
       countIf(timeSeriesGroupToTags(result) != [('dest', toString(pair_index)), ('src', toString(99 - pair_index))])
FROM numbers(1400)
GROUP BY unique_pairs
ORDER BY unique_pairs
SETTINGS max_threads = 1, max_block_size = 100;

SELECT 'crossed pairs with repeated components';
WITH
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number, timeSeriesTagsToGroup([('dest', toString(number))]) AS group
            FROM numbers(8)
            ORDER BY number
        )
    ) AS dest_groups,
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number, timeSeriesTagsToGroup([('src', toString(number))]) AS group
            FROM numbers(8)
            ORDER BY number
        )
    ) AS src_groups,
    (number * 37 + 11) % 64 AS pair_index,
    pair_index % 8 AS dest_index,
    intDiv(pair_index, 8) AS src_index,
    timeSeriesCopyTags(dest_groups[dest_index + 1], src_groups[src_index + 1], ['src']) AS result
SELECT count(), uniqExact(result),
       countIf(timeSeriesGroupToTags(result) != [('dest', toString(dest_index)), ('src', toString(src_index))])
FROM numbers(128)
SETTINGS max_threads = 1, max_block_size = 128;

SELECT 'mixed dense and sparse components';
WITH
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number, timeSeriesTagsToGroup([('dest', toString(number))]) AS group
            FROM numbers(64)
            ORDER BY number
        )
    ) AS dest_groups,
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number, timeSeriesTagsToGroup([('src', toString(number))]) AS group
            FROM numbers(64)
            ORDER BY number
        )
    ) AS src_groups,
    intDiv(number, 64) AS shape,
    (number * 13 + 3) % 16 AS pair_index,
    (pair_index % 4) * if(shape = 0, 1, 16) AS dest_index,
    intDiv(pair_index, 4) * if(shape = 1, 1, 16) AS src_index,
    timeSeriesCopyTags(dest_groups[dest_index + 1], src_groups[src_index + 1], ['src']) AS result
SELECT shape, count(), uniqExact(result),
       countIf(timeSeriesGroupToTags(result) != [('dest', toString(dest_index)), ('src', toString(src_index))])
FROM numbers(192)
GROUP BY shape
ORDER BY shape
SETTINGS max_threads = 1, max_block_size = 64;

SELECT 'dense component range boundary';
WITH
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number, timeSeriesTagsToGroup([('dest', toString(number))]) AS group
            FROM numbers(8)
            ORDER BY number
        )
    ) AS dest_groups,
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number, timeSeriesTagsToGroup([('src', toString(number))]) AS group
            FROM numbers(8)
            ORDER BY number
        )
    ) AS src_groups,
    intDiv(number, 16) AS shape,
    (number * 3 + 1) % 4 AS pair_index,
    if(shape = 0, pair_index, if(pair_index = 3, 4, pair_index)) AS dest_index,
    3 - pair_index AS src_index,
    timeSeriesCopyTags(dest_groups[dest_index + 1], src_groups[src_index + 1], ['src']) AS result
SELECT shape, count(), uniqExact(result),
       countIf(timeSeriesGroupToTags(result) != [('dest', toString(dest_index)), ('src', toString(src_index))])
FROM numbers(32)
GROUP BY shape
ORDER BY shape
SETTINGS max_threads = 1, max_block_size = 16;

SELECT 'group zero and distinct pairs with identical results';
WITH
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number, timeSeriesTagsToGroup([('dest', toString(number))]) AS group
            FROM numbers(4)
            ORDER BY number
        )
    ) AS dest_groups,
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number, timeSeriesTagsToGroup([('src', toString(number))]) AS group
            FROM numbers(4)
            ORDER BY number
        )
    ) AS src_groups,
    if(number % 5 = 0, toUInt64(0), dest_groups[number % 4 + 1]) AS dest_group,
    if(number % 7 = 0, toUInt64(0), src_groups[intDiv(number, 4) % 4 + 1]) AS src_group,
    timeSeriesCopyTags(dest_group, src_group, ['dest']) AS result
SELECT count(), uniqExact(result), countIf(timeSeriesGroupToTags(result) != [])
FROM numbers(64)
SETTINGS max_threads = 1, max_block_size = 64;

SELECT 'copyTag no-op preserves destination groups';
WITH
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number, timeSeriesTagsToGroup([('dest', toString(number))]) AS group
            FROM numbers(4)
            ORDER BY number
        )
    ) AS dest_groups,
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number, timeSeriesTagsToGroup([('src', toString(number))]) AS group
            FROM numbers(4)
            ORDER BY number
        )
    ) AS src_groups,
    dest_groups[number % 4 + 1] AS dest_group,
    src_groups[intDiv(number, 4) % 4 + 1] AS src_group,
    timeSeriesCopyTag(dest_group, src_group, 'missing') AS result
SELECT count(), uniqExact(result), countIf(result != dest_group)
FROM numbers(64)
SETTINGS max_threads = 1, max_block_size = 64;

SELECT 'vectorized single-component inputs';
WITH
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number, timeSeriesTagsToGroup([('dest', toString(number))]) AS group
            FROM numbers(8)
            ORDER BY number
        )
    ) AS dest_groups,
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number, timeSeriesTagsToGroup([('src', toString(number))]) AS group
            FROM numbers(8)
            ORDER BY number
        )
    ) AS src_groups,
    timeSeriesCopyTags(materialize(dest_groups[1]), src_groups[number % 8 + 1], ['src']) AS left_result,
    timeSeriesCopyTags(dest_groups[number % 8 + 1], materialize(src_groups[1]), ['src']) AS right_result
SELECT count(),
       countIf(timeSeriesGroupToTags(left_result) != [('dest', '0'), ('src', toString(number % 8))]),
       countIf(timeSeriesGroupToTags(right_result) != [('dest', toString(number % 8)), ('src', '0')])
FROM numbers(64)
SETTINGS max_threads = 1, max_block_size = 64;

SELECT 'empty vectorized input';
SELECT count() FROM
(
    SELECT timeSeriesCopyTags(number, number, ['src']) FROM numbers(0)
);

-- Invalid IDs must be rejected before indexing the collector or allocating their numeric range.
SELECT timeSeriesCopyTags(materialize(toUInt64(0)), materialize(toUInt64(18446744073709551615)), ['src']); -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesCopyTags(materialize(toUInt64(18446744073709551615)), materialize(toUInt64(0)), ['src']); -- {serverError BAD_ARGUMENTS}
