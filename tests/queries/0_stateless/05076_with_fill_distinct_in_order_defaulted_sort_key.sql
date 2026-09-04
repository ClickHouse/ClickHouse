-- A generated `WITH FILL` row writes a default value into every `ORDER BY` key that is neither filled nor
-- part of the filling sorting prefix, so the stream stops being ordered by that key and `DISTINCT` in
-- order must not run `DistinctSortedStreamTransform` over it.
-- The labelled line after each result reports (sorted transform chosen, hash transform chosen). C1, C2 and
-- D are sensitivity controls: shapes that do stay ordered must keep the sorted transform, so refusing the
-- optimization for every `WITH FILL` query also fails this test.

-- A: two `WITH FILL` keys with a non-fill key between them, so generated rows default `s`.
SELECT DISTINCT * FROM (
    SELECT * FROM values('i Int64, s String, d Int64', (1, 'a', 1), (1, 'a', 5))
    ORDER BY i ASC WITH FILL, s ASC, d ASC WITH FILL
) SETTINGS optimize_distinct_in_order = 1;
SELECT 'A', countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0, countIf(explain ILIKE '%DistinctTransform%') > 0
FROM (EXPLAIN PIPELINE
    SELECT DISTINCT * FROM (
        SELECT * FROM values('i Int64, s String, d Int64', (1, 'a', 1), (1, 'a', 5))
        ORDER BY i ASC WITH FILL, s ASC, d ASC WITH FILL
    )
) SETTINGS optimize_distinct_in_order = 1;

-- A2: the same shape projected so that a key recurs non-contiguously. The per-range hash table is reset,
-- so an unsorted stream emits `1 a` twice. Two rows, and this is the one block that still fails in a
-- build where the contiguity assertion is compiled out.
SELECT DISTINCT i, s FROM (
    SELECT * FROM values('i Int64, s String, d Int64', (1, 'a', 1), (1, 'a', 5))
    ORDER BY i ASC WITH FILL, s ASC, d ASC WITH FILL
) SETTINGS optimize_distinct_in_order = 1;

-- B: `use_with_fill_by_sorting_prefix = 0` builds no prefix, so a key ahead of the fill key is defaulted too.
SELECT DISTINCT * FROM (
    SELECT * FROM values('s String, i Int64', ('a', 1), ('b', 3), ('a', 5))
    ORDER BY s ASC, i ASC WITH FILL
) SETTINGS optimize_distinct_in_order = 1, use_with_fill_by_sorting_prefix = 0;
SELECT 'B', countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0, countIf(explain ILIKE '%DistinctTransform%') > 0
FROM (EXPLAIN PIPELINE
    SELECT DISTINCT * FROM (
        SELECT * FROM values('s String, i Int64', ('a', 1), ('b', 3), ('a', 5))
        ORDER BY s ASC, i ASC WITH FILL
    )
) SETTINGS optimize_distinct_in_order = 1, use_with_fill_by_sorting_prefix = 0;

-- C1: every `ORDER BY` key is filled, so nothing is defaulted and the order survives.
SELECT DISTINCT * FROM (
    SELECT * FROM values('i Int64, d Int64', (1, 1), (1, 3))
    ORDER BY i ASC WITH FILL, d ASC WITH FILL
) SETTINGS optimize_distinct_in_order = 1;
SELECT 'C1', countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0, countIf(explain ILIKE '%DistinctTransform%') > 0
FROM (EXPLAIN PIPELINE
    SELECT DISTINCT * FROM (
        SELECT * FROM values('i Int64, d Int64', (1, 1), (1, 3))
        ORDER BY i ASC WITH FILL, d ASC WITH FILL
    )
) SETTINGS optimize_distinct_in_order = 1;

-- C2: a single fill key with nothing after it.
SELECT DISTINCT * FROM (
    SELECT * FROM values('i Int64', (1), (3))
    ORDER BY i ASC WITH FILL
) SETTINGS optimize_distinct_in_order = 1;
SELECT 'C2', countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0, countIf(explain ILIKE '%DistinctTransform%') > 0
FROM (EXPLAIN PIPELINE
    SELECT DISTINCT * FROM (
        SELECT * FROM values('i Int64', (1), (3))
        ORDER BY i ASC WITH FILL
    )
) SETTINGS optimize_distinct_in_order = 1;

-- D: the same `ORDER BY` as B, but the prefix is built and copied into generated rows, so the order survives.
SELECT DISTINCT * FROM (
    SELECT * FROM values('s String, i Int64', ('a', 1), ('b', 3), ('a', 5))
    ORDER BY s ASC, i ASC WITH FILL
) SETTINGS optimize_distinct_in_order = 1;
SELECT 'D', countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0, countIf(explain ILIKE '%DistinctTransform%') > 0
FROM (EXPLAIN PIPELINE
    SELECT DISTINCT * FROM (
        SELECT * FROM values('s String, i Int64', ('a', 1), ('b', 3), ('a', 5))
        ORDER BY s ASC, i ASC WITH FILL
    )
) SETTINGS optimize_distinct_in_order = 1;

-- E: a non-filled key after the last fill key. A generated row differs from its neighbours on the fill
-- key, so a comparison never reaches `s` and the order survives.
SELECT DISTINCT * FROM (
    SELECT * FROM values('i Int64, s String', (1, 'a'), (1, 'b'), (3, 'c'))
    ORDER BY i ASC WITH FILL, s ASC
) SETTINGS optimize_distinct_in_order = 1;
SELECT 'E', countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0, countIf(explain ILIKE '%DistinctTransform%') > 0
FROM (EXPLAIN PIPELINE
    SELECT DISTINCT * FROM (
        SELECT * FROM values('i Int64, s String', (1, 'a'), (1, 'b'), (3, 'c'))
        ORDER BY i ASC WITH FILL, s ASC
    )
) SETTINGS optimize_distinct_in_order = 1;
