-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/109877
-- ORDER BY <LowCardinality column> LIMIT N after LEFT JOIN with lazy column
-- replication produced a permutation of size min(rows, limit) = LIMIT, which
-- ColumnReplicated::permute (from the JOIN payload columns) rejected with
-- SIZES_OF_COLUMNS_DOESNT_MATCH. Fixed on master by routing sortBlock through
-- transformColumnsWithSharedIndex (PR #106566); this PR additionally relaxes the
-- overly strict size check in ColumnReplicated::permute so the general
-- IColumn::permute contract (a shorter permutation with a limit) is accepted.

SET enable_lazy_columns_replication = 1;

-- den-crane's minimal repro: single LowCardinality sort key, LIMIT below row count,
-- two LEFT JOINs so the payload columns become ColumnReplicated.
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
-- the settings under which the pre-fix binary threw deterministically.
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
