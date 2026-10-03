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
    [1, 19, 20, 21, 99, 100] AS cutoffs,
    cutoffs[intDiv(number, 100) + 1] AS unique_pairs,
    (number * 37 + 11) % 100 AS row_index,
    if(row_index < unique_pairs, row_index, 0) AS pair_index,
    timeSeriesCopyTags(dest_groups[pair_index + 1], src_groups[100 - pair_index], ['src']) AS result,
    timeSeriesCopyTag(dest_groups[pair_index + 1], src_groups[100 - pair_index], 'missing') AS no_op_result
SELECT unique_pairs, count(), uniqExact(result),
       countIf(timeSeriesGroupToTags(result) != [('dest', toString(pair_index)), ('src', toString(99 - pair_index))]
               OR no_op_result != dest_groups[pair_index + 1])
FROM numbers(600)
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
FROM numbers(64)
SETTINGS max_threads = 1, max_block_size = 64;

SELECT 'mixed dense and sparse components';
WITH
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number, timeSeriesTagsToGroup([('dest', toString(number))]) AS group
            FROM numbers(128)
            ORDER BY number
        )
    ) AS dest_groups,
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number, timeSeriesTagsToGroup([('src', toString(number))]) AS group
            FROM numbers(128)
            ORDER BY number
        )
    ) AS src_groups,
    intDiv(number, 128) AS shape,
    (number * 13 + 3) % 16 AS pair_index,
    (pair_index % 4) * if(shape = 0, 1, 32) AS dest_index,
    intDiv(pair_index, 4) * if(shape = 1, 1, 32) AS src_index,
    timeSeriesCopyTags(dest_groups[dest_index + 1], src_groups[src_index + 1], ['src']) AS result
SELECT shape, count(), uniqExact(result),
       countIf(timeSeriesGroupToTags(result) != [('dest', toString(dest_index)), ('src', toString(src_index))])
FROM numbers(384)
GROUP BY shape
ORDER BY shape
SETTINGS max_threads = 1, max_block_size = 128;

SELECT 'dense component range boundary';
-- Four unique pairs in 32 rows select component deduplication. Destination ranges
-- 15 and 16 fall immediately below and at its range / unique_pairs == 4 boundary.
WITH
    (
        SELECT groupArray(group)
        FROM
        (
            SELECT number, timeSeriesTagsToGroup([('dest', toString(number))]) AS group
            FROM numbers(17)
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
    intDiv(number, 32) AS shape,
    (number * 3 + 1) % 4 AS pair_index,
    if(pair_index = 3, 15 + shape, pair_index) AS dest_index,
    3 - pair_index AS src_index,
    timeSeriesCopyTags(dest_groups[dest_index + 1], src_groups[src_index + 1], ['src']) AS result
SELECT shape, count(), uniqExact(result),
       countIf(timeSeriesGroupToTags(result) != [('dest', toString(dest_index)), ('src', toString(src_index))])
FROM numbers(64)
GROUP BY shape
ORDER BY shape
SETTINGS max_threads = 1, max_block_size = 32;

SELECT 'unique inputs with identical results';
WITH
    timeSeriesTagsToGroup([('dest', toString(number))]) AS dest_group,
    timeSeriesTagsToGroup([('src', toString(number))]) AS src_group,
    timeSeriesCopyTags(dest_group, src_group, ['dest']) AS pair_result,
    timeSeriesRemoveTag(dest_group, 'dest') AS unary_result
SELECT count(), uniqExact(pair_result), uniqExact(unary_result),
       countIf(timeSeriesGroupToTags(pair_result) != [] OR timeSeriesGroupToTags(unary_result) != [])
FROM numbers(16)
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

-- Exercise repeated, dense, sparse and late-invalid IDs in a full vectorized block.
-- These queries create no tags, so group 0 is the only valid group.
SELECT timeSeriesRemoveTag(materialize(toUInt64(1000000)), 'missing')
FROM numbers(4096) SETTINGS max_threads = 1, max_block_size = 4096 FORMAT Null; -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesRemoveTag(number + 1000000, 'missing')
FROM numbers(4096) SETTINGS max_threads = 1, max_block_size = 4096 FORMAT Null; -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesRemoveTag(if(number % 2 = 0, toUInt64(0), toUInt64(18446744073709551615)), 'missing')
FROM numbers(4096) SETTINGS max_threads = 1, max_block_size = 4096 FORMAT Null; -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesRemoveTag(if(number = 4095, toUInt64(1), toUInt64(0)), 'missing')
FROM numbers(4096) SETTINGS max_threads = 1, max_block_size = 4096 FORMAT Null; -- {serverError BAD_ARGUMENTS}

-- Keep both copy arguments vectorized and validate each side before pair/component indexing.
SELECT timeSeriesCopyTags(materialize(toUInt64(1000000)), materialize(toUInt64(0)), ['src'])
FROM numbers(4096) SETTINGS max_threads = 1, max_block_size = 4096 FORMAT Null; -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesCopyTags(materialize(toUInt64(0)), materialize(toUInt64(1000000)), ['src'])
FROM numbers(4096) SETTINGS max_threads = 1, max_block_size = 4096 FORMAT Null; -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesCopyTags(number + 1000000, materialize(toUInt64(0)), ['src'])
FROM numbers(4096) SETTINGS max_threads = 1, max_block_size = 4096 FORMAT Null; -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesCopyTags(materialize(toUInt64(0)), number + 1000000, ['src'])
FROM numbers(4096) SETTINGS max_threads = 1, max_block_size = 4096 FORMAT Null; -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesCopyTags(if(number % 2 = 0, toUInt64(0), toUInt64(18446744073709551615)), materialize(toUInt64(0)), ['src'])
FROM numbers(4096) SETTINGS max_threads = 1, max_block_size = 4096 FORMAT Null; -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesCopyTags(materialize(toUInt64(0)), if(number % 2 = 0, toUInt64(0), toUInt64(18446744073709551615)), ['src'])
FROM numbers(4096) SETTINGS max_threads = 1, max_block_size = 4096 FORMAT Null; -- {serverError BAD_ARGUMENTS}

-- Even a no-op copy must reject an invalid final row on either side.
SELECT timeSeriesCopyTag(if(number = 4095, toUInt64(1), toUInt64(0)), materialize(toUInt64(0)), 'missing')
FROM numbers(4096) SETTINGS max_threads = 1, max_block_size = 4096 FORMAT Null; -- {serverError BAD_ARGUMENTS}
SELECT timeSeriesCopyTag(materialize(toUInt64(0)), if(number = 4095, toUInt64(1), toUInt64(0)), 'missing')
FROM numbers(4096) SETTINGS max_threads = 1, max_block_size = 4096 FORMAT Null; -- {serverError BAD_ARGUMENTS}
