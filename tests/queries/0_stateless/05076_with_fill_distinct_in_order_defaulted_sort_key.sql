-- A generated `WITH FILL` row writes a default value into every `ORDER BY` key that is neither filled nor
-- part of the filling sorting prefix, so the stream stops being ordered by that key and `DISTINCT` in order
-- must not group by it. The keys ahead of it stay ordered and must keep driving the optimization.
-- The labelled line after each result reports (sorted transform chosen, hash transform chosen). C1, C2, D
-- and F are sensitivity controls: shapes that do stay ordered must keep the optimization, so refusing it
-- for every `WITH FILL` query also fails this test.

-- A: two `WITH FILL` keys with a non-fill key between them, so generated rows default `s`. The order by `i`
-- survives and still drives the sorted transform; only `s` must be left out of it, which is what A2 detects.
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

-- F: `DISTINCT` on a key ahead of the defaulted one. That key is still ordered, so the sorted transform
-- must survive: it hashes nothing here, while the hash transform charges `max_bytes_in_distinct`.
SELECT count() FROM (
    SELECT DISTINCT i FROM (
        SELECT number * 2 AS i, 'x' AS s, 1 AS d FROM numbers(1000)
        ORDER BY i ASC WITH FILL, s ASC, d ASC WITH FILL
    )
) SETTINGS optimize_distinct_in_order = 1, max_bytes_in_distinct = 1024;
SELECT 'F', countIf(explain ILIKE '%DistinctSortedStreamTransform%') > 0, countIf(explain ILIKE '%DistinctTransform%') > 0
FROM (EXPLAIN PIPELINE
    SELECT DISTINCT i FROM (
        SELECT number * 2 AS i, 'x' AS s, 1 AS d FROM numbers(1000)
        ORDER BY i ASC WITH FILL, s ASC, d ASC WITH FILL
    )
) SETTINGS optimize_distinct_in_order = 1;

-- H: a window over B's shape. Its own sort must not be skipped: peers are found by comparing neighbours.
SELECT o, number, rank() OVER w AS r FROM (
    SELECT number, number % 2 AS o FROM numbers(6)
    ORDER BY o ASC, number ASC WITH FILL
) WINDOW w AS (ORDER BY o ASC, number ASC)
ORDER BY o ASC, number ASC, r ASC
SETTINGS use_with_fill_by_sorting_prefix = 0, optimize_sorting_by_input_stream_properties = 1;

-- I: an outer `ORDER BY` over the same shape must still sort.
SELECT o, number FROM (
    SELECT number, number % 2 AS o FROM numbers(6)
    ORDER BY o ASC, number ASC WITH FILL
) ORDER BY o ASC, number ASC
SETTINGS use_with_fill_by_sorting_prefix = 0, optimize_sorting_by_input_stream_properties = 1;

-- J: a `Float32` fill key. Fill values are computed in `Float64`, so the generated `0.3` rounds onto the original one and
-- the keys after it lose their order, whether filled or not. An outer `ORDER BY` must still sort them.
SELECT x, s FROM (
    SELECT * FROM values('x Float32, s String', (0, 'a'), (0.3, 'a'))
    ORDER BY x ASC WITH FILL STEP 0.1, s DESC
) ORDER BY x ASC, s DESC
SETTINGS optimize_sorting_by_input_stream_properties = 1;
SELECT x, d FROM (
    SELECT * FROM values('x Float32, d Int64', (0, 1), (0.3, 1))
    ORDER BY x ASC WITH FILL STEP 0.1, d DESC WITH FILL FROM -5
) ORDER BY x ASC, d DESC
SETTINGS optimize_sorting_by_input_stream_properties = 1;

-- K: a `BFloat16` fill key, where the generated values round down onto the original `1`.
SELECT x, s FROM (
    SELECT * FROM values('x BFloat16, s String', (1, 'a'), (1.0078125, 'a'))
    ORDER BY x ASC WITH FILL STEP 0.001, s ASC
) ORDER BY x ASC, s ASC
SETTINGS optimize_sorting_by_input_stream_properties = 1;
