-- Tags: no-parallel-replicas
-- ^ The query must run as a single partial-sort stream (max_threads = 1, no parallel replicas) so
--   that PartialSortingTransform processes the blocks in sequence, which is what triggers the bug.

-- Regression test for a wrong-result / debug LOGICAL_ERROR
-- ("Rows are not sorted with permutation") in multi-column ORDER BY ... LIMIT
-- over Nullable sort columns. See https://github.com/ClickHouse/ClickHouse/issues/104376
--
-- ColumnNullable::updatePermutationImpl returned an `equal_ranges` vector that was not sorted by
-- position when several input ranges were each split into NULL / non-NULL parts (it concatenated
-- all NULL ranges and all non-NULL ranges instead of merging them). The limit handling downstream
-- assumes `equal_ranges` is ordered by `from`, so it dropped some below-limit equal ranges, leaving
-- the rows in them unsorted by the following sort columns. On debug builds the sortBlock self-check
-- aborted with a LOGICAL_ERROR; on release builds the query returned rows in the wrong order.
--
-- The data below ties many rows on the leading columns and differs only on later columns, and is
-- fed through PartialSortingTransform as several blocks (max_block_size < number of rows) with a
-- LIMIT, which is the exact shape that triggered the failure. Before the fix this query aborts the
-- server in debug builds; after the fix it completes normally.

SELECT
    if(number % 3 = 0, ['a', 'b'], emptyArrayString())                          AS c1, -- Array(String)
    if(number % 5 = 0, toNullable(toString(cityHash64(number, 10) % 4)), NULL)  AS c2, -- Nullable(String)
    toString(cityHash64(number * 3, 10) % 12)                                   AS c3, -- String
    if(number % 5 = 0, toNullable(toString(number % 3)), NULL)                  AS c4, -- Nullable(String)
    toString(number % 4)                                                        AS c5  -- String
FROM numbers(1170)
ORDER BY ALL DESC NULLS FIRST
LIMIT 150
FORMAT Null
SETTINGS max_block_size = 360, max_threads = 1;

SELECT 'ok';
