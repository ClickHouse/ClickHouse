-- `max_streams_for_union_step = 1` makes `narrowPipe` glue every output port of the UNION behind a
-- single `ConcatProcessor`, which demands its inputs one at a time. A `ScatterByPartitionTransform`
-- above such a Concat used to wedge the pipeline: it cannot consume its next chunk until every
-- output has taken a share, and the lanes feeding the Concat's later inputs are never demanded.

-- Sharded merge join: scatter fan-out under the glued UNION. Used to abort with
-- `Logical error: Pipeline stuck`.
SELECT count() FROM
(
    SELECT l.a FROM (SELECT number % 64 AS a FROM numbers_mt(1000)) AS l
        INNER JOIN (SELECT number % 64 AS a FROM numbers_mt(1000)) AS r ON l.a = r.a
    UNION ALL
    SELECT l.a FROM (SELECT number % 64 AS a FROM numbers_mt(1000)) AS l
        INNER JOIN (SELECT number % 64 AS a FROM numbers_mt(1000)) AS r ON l.a = r.a
)
SETTINGS max_threads = 4, max_streams_for_union_step = 1,
         max_streams_for_union_step_to_max_threads_ratio = 0,
         join_algorithm = 'parallel_full_sorting_merge';

-- The narrowing is what is skipped, not the query: no Concat is built above the scatter.
SELECT count() FROM
(
    EXPLAIN PIPELINE
    SELECT count() FROM
    (
        SELECT l.a FROM (SELECT number % 64 AS a FROM numbers_mt(1000)) AS l
            INNER JOIN (SELECT number % 64 AS a FROM numbers_mt(1000)) AS r ON l.a = r.a
        UNION ALL
        SELECT l.a FROM (SELECT number % 64 AS a FROM numbers_mt(1000)) AS l
            INNER JOIN (SELECT number % 64 AS a FROM numbers_mt(1000)) AS r ON l.a = r.a
    )
    SETTINGS max_threads = 4, max_streams_for_union_step = 1,
             max_streams_for_union_step_to_max_threads_ratio = 0,
             join_algorithm = 'parallel_full_sorting_merge'
)
WHERE explain ILIKE '%Concat%';

-- Same for the other producer of the fan-out, a window function with PARTITION BY.
SELECT count() FROM
(
    EXPLAIN PIPELINE
    SELECT count() FROM
    (
        SELECT row_number() OVER (PARTITION BY a ORDER BY b) AS r
            FROM (SELECT number % 64 AS a, number AS b FROM numbers_mt(4000000))
        UNION ALL
        SELECT row_number() OVER (PARTITION BY a ORDER BY b) AS r
            FROM (SELECT number % 64 AS a, number AS b FROM numbers_mt(4000000))
    )
    SETTINGS max_threads = 4, max_streams_for_union_step = 1,
             max_streams_for_union_step_to_max_threads_ratio = 0
)
WHERE explain ILIKE '%Concat%';

-- Negative control: a UNION with no fan-out in it is still narrowed.
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT count() FROM
    (
        SELECT number FROM numbers_mt(1000) UNION ALL SELECT number FROM numbers_mt(1000)
    )
    SETTINGS max_threads = 4, max_streams_for_union_step = 1,
             max_streams_for_union_step_to_max_threads_ratio = 0
)
WHERE explain ILIKE '%Concat%';
