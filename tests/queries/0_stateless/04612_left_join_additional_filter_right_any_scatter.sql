-- A `LEFT JOIN` whose right table has unique keys is promoted to `RightAny`. With a non-equi `ON`
-- condition the join stops collecting candidates once `max_joined_block_size_rows` is reached, so
-- the left block is truncated to that prefix while the negated null map used to build the
-- required right key column still had the full block size. Applying it to the shorter left key
-- column raised a size-mismatch `LOGICAL_ERROR`.

-- The old analyzer rejects the non-equi `ON` condition with `INVALID_JOIN_ON_EXPRESSION`. Swapping
-- the tables makes this a `RIGHT ALL` join, which is not promoted to `RightAny`; it can still stop
-- at the candidate limit, but that path replicates and so has always resized the filter.
-- `join_use_nulls` decides whether the right key becomes `Nullable`, which selects which of the two
-- errors below is raised, so it is pinned per query.
SET enable_analyzer = 1;
SET query_plan_join_swap_table = false;
SET use_query_cache = 0;

-- Raised `Null map of size 1000 at offset 0 does not match ColumnNullable of size 256`.
SELECT count(), sum(coalesce(r.a, 0)), sum(coalesce(r.b, 0))
FROM (SELECT number % 2 AS k FROM numbers(1000)) AS l
LEFT JOIN (SELECT toNullable(number) AS a, number AS b FROM numbers(2)) AS r
  ON (l.k = r.a) AND (l.k > r.b)
SETTINGS join_algorithm = 'hash', max_joined_block_size_rows = 256, join_use_nulls = 1,
         log_comment = '04612_nullable';

-- Same shape with a non-nullable right key hit the sibling `Invalid number of rows in Chunk`.
SELECT count(), sum(coalesce(r.a, 0)), sum(coalesce(r.b, 0))
FROM (SELECT number % 2 AS k FROM numbers(1000)) AS l
LEFT JOIN (SELECT number AS a, number AS b FROM numbers(2)) AS r
  ON (l.k = r.a) AND (l.k > r.b)
SETTINGS join_algorithm = 'hash', max_joined_block_size_rows = 256, join_use_nulls = 0;

-- The two queries above join no rows, so also cover the case where the `ON` condition holds.
SELECT count(), sum(coalesce(r.a, 0)), sum(coalesce(r.b, 0))
FROM (SELECT number % 2 AS k FROM numbers(1000)) AS l
LEFT JOIN (SELECT toNullable(number) AS a, number - 1 AS b FROM numbers(2)) AS r
  ON (l.k = r.a) AND (l.k > r.b)
SETTINGS join_algorithm = 'hash', max_joined_block_size_rows = 256, join_use_nulls = 1;

-- The aggregates above are the same whether or not the two conditions the bug needs hold, so
-- assert both of them separately.
SYSTEM FLUSH LOGS query_log, text_log;

-- The right table is the build side only while the join is not swapped. Splitting the probe
-- candidates at max_joined_block_size_rows must not resubmit the unprocessed left suffix: each
-- of the 1000 left rows is accounted for exactly once.
SELECT build_rows = 2 AND probe_rows = 1000 AS split
FROM (
    SELECT ProfileEvents['JoinBuildTableRowCount'] AS build_rows,
           ProfileEvents['JoinProbeTableRowCount'] AS probe_rows
    FROM system.query_log
    WHERE type = 'QueryFinish' AND current_database = currentDatabase()
      AND log_comment = '04612_nullable'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
);

-- `HashJoin::onBuildPhaseFinish` decides the promotion from the data alone, and it is reported by
-- no setting, profile event or result, so assert the message it logs. The counters above cannot
-- stand in for it: a `LEFT ALL` join on the same unique right keys reads the same 2 build rows and
-- splits the same way.
SELECT count() > 0 AS promoted
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND message LIKE '%Promoting join strictness to RightAny%'
  AND query_id IN (
      SELECT query_id
      FROM system.query_log
      WHERE type = 'QueryFinish' AND current_database = currentDatabase()
        AND log_comment = '04612_nullable' AND event_date >= yesterday()
      ORDER BY event_time_microseconds DESC
      LIMIT 1
  );

-- Multi-disjunct joins use several hash maps and can stop probing a left block at the candidate
-- limit as well. Continuation must retain the scattered suffix instead of materializing and
-- submitting it as a new probe block.
SELECT l.n
FROM
(
    SELECT number AS n, number % 4 AS k1, number % 8 AS k2
    FROM numbers(1000)
) AS l
ALL INNER JOIN
(
    SELECT number AS n, number % 4 AS k1, number % 8 AS k2
    FROM numbers(64)
) AS r
ON l.k1 = r.k1 OR l.k2 = r.k2
FORMAT Null
SETTINGS join_algorithm = 'hash',
         max_threads = 1,
         max_block_size = 1000,
         max_joined_block_size_rows = 64,
         query_plan_join_swap_table = 'false',
         log_comment = '04612_multi_disjunct';

SYSTEM FLUSH LOGS query_log;

SELECT probe_rows = 1000 AS probe_once
FROM
(
    SELECT ProfileEvents['JoinProbeTableRowCount'] AS probe_rows
    FROM system.query_log
    WHERE type = 'QueryFinish'
      AND current_database = currentDatabase()
      AND log_comment = '04612_multi_disjunct'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
);
