-- `ConstantJoin` shrinks the stored right-side blocks to fit once their size exceeds half of `max_bytes_in_join`.
-- The right side is around 20 MiB (also when allocations are inflated by sanitizers), so with the 30 MB limit
-- the shrinking starts mid-build and later blocks are stored already shrunk.
-- Compression is disabled explicitly: compressed stored blocks would stay below the shrinking threshold.

SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;

SELECT count(), sum(l.number), sum(r.number), min(length(r.s))
FROM numbers(2) AS l
CROSS JOIN (SELECT number, repeat('x', 100) AS s FROM numbers(200000)) AS r
SETTINGS max_bytes_in_join = 30000000, cross_join_min_rows_to_compress = 0, cross_join_min_bytes_to_compress = 0,
         log_comment = '04546_constant_join_shrink';

-- Make sure the stored blocks were actually shrunk.
SYSTEM FLUSH LOGS query_log, text_log;

SELECT count() > 0 AS shrunk_stored_blocks
FROM system.text_log
WHERE (event_date >= yesterday())
AND (event_time >= (now() - 60))
AND (query_id IN
(
    SELECT query_id
        FROM system.query_log
        WHERE (log_comment = '04546_constant_join_shrink')
        AND (current_database = currentDatabase())
        AND (type = 'QueryFinish')
        AND (event_date >= yesterday())
    )
)
AND (logger_name = 'ConstantJoin')
AND (message LIKE 'Shrinking stored blocks%');

-- A query that triggered NOT_IMPLEMENTED to be thrown because it tried to shrink ColumnReplicated
SELECT count(), min(length(r.s))
FROM numbers(2) AS l
CROSS JOIN
(
    SELECT number, concat(randomPrintableASCII(60), repeat('x', 60)) AS s
    FROM numbers(300000)
) AS r
SETTINGS
    max_bytes_in_join = 30000000,
    cross_join_min_rows_to_compress = 1,
    cross_join_min_bytes_to_compress = 1,
    query_plan_join_swap_table = 0;
