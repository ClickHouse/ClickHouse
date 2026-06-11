-- Tags: no-parallel-replicas
-- ^ The query must run as a single partial-sort stream (max_threads = 1, no parallel replicas) so
--   that PartialSortingTransform processes the blocks in sequence, which is what triggers the bug.

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104376 (unsorted
-- equal_ranges from ColumnNullable in multi-column ORDER BY ... LIMIT). The data ties many rows on
-- the leading columns, fed through PartialSortingTransform as several blocks with a LIMIT.

-- Original shape: aborts the server on debug builds before the fix.
SELECT
    if(number % 3 = 0, ['a', 'b'], emptyArrayString())                          AS c1,
    if(number % 5 = 0, toNullable(toString(cityHash64(number, 10) % 4)), NULL)  AS c2,
    toString(cityHash64(number * 3, 10) % 12)                                   AS c3,
    if(number % 5 = 0, toNullable(toString(number % 3)), NULL)                  AS c4,
    toString(number % 4)                                                        AS c5
FROM numbers(1170)
ORDER BY ALL DESC NULLS FIRST
LIMIT 150
FORMAT Null
SETTINGS max_block_size = 360, max_threads = 1;

-- Release coverage: assert the multi-block partial-sort top-N matches a trusted single-block sort.
-- The comparison is over the sort-key values, so ties do not make it flaky. With the bug the
-- multi-block result differs (at this limit the dropped ranges fall inside the visible top-N).
WITH base AS
(
    SELECT
        if(number % 3 = 0, ['a', 'b'], emptyArrayString())                          AS c1,
        if(number % 5 = 0, toNullable(toString(cityHash64(number, 10) % 4)), NULL)  AS c2,
        toString(cityHash64(number * 3, 10) % 12)                                   AS c3,
        if(number % 5 = 0, toNullable(toString(number % 3)), NULL)                  AS c4,
        toString(number % 4)                                                        AS c5
    FROM numbers(1170)
)
SELECT
    (
        SELECT groupArray((c1, c2, c3, c4, c5))
        FROM (SELECT c1, c2, c3, c4, c5 FROM base ORDER BY ALL DESC NULLS FIRST LIMIT 400
              SETTINGS max_block_size = 360, max_threads = 1)
    )
    =
    (
        SELECT groupArray((c1, c2, c3, c4, c5))
        FROM (SELECT c1, c2, c3, c4, c5 FROM base ORDER BY ALL DESC NULLS FIRST LIMIT 400
              SETTINGS max_block_size = 100000, max_threads = 1)
    ) AS partial_sort_matches_full_sort;
