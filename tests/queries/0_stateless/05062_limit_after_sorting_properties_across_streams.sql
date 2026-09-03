-- `LimitRangeStep` evaluates its range over a single stream. When its input consists of several
-- streams that are each sorted (a `UNION ALL` of ordered branches), the streams are concatenated in
-- arbitrary order, so the step must not advertise the per-stream order to the optimizer above it:
-- otherwise the outer `ORDER BY` would be reduced to a finish sort over an unsorted stream. A globally
-- sorted input keeps its order across the step, so there the outer sort still becomes a finish sort.
SET optimize_sorting_by_input_stream_properties = 1;
SET max_threads = 4;
SET max_block_size = 1;
SET explain_query_plan_default = 'legacy';

SELECT x
FROM
(
    SELECT x
    FROM
    (
        (SELECT number * 2 AS x FROM numbers(20) ORDER BY x)
        UNION ALL
        (SELECT number * 2 + 1 AS x FROM numbers(20) ORDER BY x)
    )
    LIMIT AFTER x >= 0
)
ORDER BY x
SETTINGS enable_analyzer = 1;

SELECT x
FROM
(
    SELECT x
    FROM
    (
        (SELECT number * 2 AS x FROM numbers(20) ORDER BY x)
        UNION ALL
        (SELECT number * 2 + 1 AS x FROM numbers(20) ORDER BY x)
    )
    LIMIT AFTER x >= 0
)
ORDER BY x
SETTINGS enable_analyzer = 0;

-- Over the union the outer sort stays a full sort (only a `Sort description`), while over one ordered
-- stream it becomes a finish sort (a `Prefix sort description`).
SELECT trimLeft(explain)
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT x
    FROM
    (
        SELECT x
        FROM
        (
            (SELECT number * 2 AS x FROM numbers(20) ORDER BY x)
            UNION ALL
            (SELECT number * 2 + 1 AS x FROM numbers(20) ORDER BY x)
        )
        LIMIT AFTER x >= 0
    )
    ORDER BY x
)
WHERE explain ILIKE '%sort description%';

SELECT trimLeft(explain)
FROM
(
    EXPLAIN PLAN actions = 1
    SELECT x
    FROM (SELECT number AS x FROM numbers(20) ORDER BY x LIMIT AFTER x >= 5)
    ORDER BY x
)
WHERE explain ILIKE '%sort description%';
