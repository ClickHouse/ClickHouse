-- End-to-end coverage for ORDER BY <LowCardinality column> LIMIT N after a
-- LEFT JOIN with lazy column replication: the query must return its rows.
-- This does not exercise the relaxed ColumnReplicated::permute contract, and
-- it is not a regression test for it: on current master sortBlock routes a
-- replicated column through transformColumnsWithSharedIndex, so the query
-- permutes the shared index and never reaches that method. The contract is
-- covered directly by the ColumnReplicated gtest cases instead.

SET enable_lazy_columns_replication = 1;

-- Single LowCardinality sort key, LIMIT below row count, two LEFT JOINs so the
-- payload columns become ColumnReplicated.
SELECT l.lc, l.s
FROM
(
    SELECT number % 10 AS k,
        toLowCardinality(toString((number * 7) % 100)) AS lc,
        concat('long_payload_string_', toString(number)) AS s
    FROM numbers(10)
) AS l
LEFT JOIN ( SELECT number % 10 AS k FROM numbers(100) ) AS r ON l.k = r.k
LEFT JOIN ( SELECT number % 10 AS k FROM numbers(100) ) AS r1 ON l.k = r1.k
ORDER BY l.lc DESC
LIMIT 5
FORMAT Null;

-- Same shape with the top-k-through-join optimization disabled and small blocks,
-- so the sort runs per block rather than once over the joined result.
SELECT count()
FROM
(
    SELECT l.lc, l.s
    FROM
    (
        SELECT number % 10 AS k,
            toLowCardinality(toString((number * 7) % 100)) AS lc,
            concat('long_payload_string_', toString(number)) AS s
        FROM numbers(10)
    ) AS l
    LEFT JOIN ( SELECT number % 10 AS k FROM numbers(100) ) AS r ON l.k = r.k
    LEFT JOIN ( SELECT number % 10 AS k FROM numbers(100) ) AS r1 ON l.k = r1.k
    ORDER BY l.lc DESC
    LIMIT 5
)
SETTINGS query_plan_top_k_through_join = 0, max_threads = 4, max_block_size = 5;
