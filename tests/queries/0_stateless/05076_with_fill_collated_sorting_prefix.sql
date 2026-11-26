-- Tags: no-fasttest
-- no-fasttest: requires ICU for collation support.

-- A collated key in the filling sorting prefix. Filling copies the prefix into generated rows, so the
-- key keeps its collated order and an outer sort over it is still avoidable. One inner sort, no outer one.
SELECT 'G', countIf(explain ILIKE '%MergeSortingTransform%')
FROM (EXPLAIN PIPELINE
    SELECT s, i, t, d FROM (
        SELECT toString(number % 3) AS s, number AS i, 'x' AS t, 1 AS d FROM numbers(1000)
        ORDER BY s ASC COLLATE 'en', i ASC WITH FILL, t ASC, d ASC WITH FILL
    )
    ORDER BY s ASC COLLATE 'en', i ASC, t ASC, d ASC
) SETTINGS max_threads = 1, optimize_sorting_by_input_stream_properties = 1;
