SET max_block_size = 65536;
SET min_joined_block_size_rows = 65536;
SET min_joined_block_size_bytes = 512000;
SET enable_lazy_columns_replication = 0;
SET query_plan_join_swap_table = false;
SET max_threads = 1;
SET enable_analyzer = 1;
-- `joined_block_split_single_row` requires a non-zero row limit, so it cannot be on here.
SET joined_block_split_single_row = 0;

-- 1000 output rows: 100 left rows, 10 right matches per key.
-- `max_joined_block_size_rows = 0` means unlimited, so a byte budget the whole result cannot
-- reach must leave the output unsplit: one block of 1000 rows.
SELECT uniqExact(bs), max(bs), count()
FROM
(
    SELECT blockSize() AS bs
    FROM (SELECT number % 10 AS k, rightPad(toString(number), 64, '_') AS v FROM numbers(100)) AS l
    INNER JOIN (SELECT number % 10 AS k, rightPad(toString(number), 64, '_') AS v2 FROM numbers(100)) AS r
    ON l.k = r.k
)
SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 4194304, join_algorithm = 'hash';

-- Same query with a row limit in range, which must still split at that limit.
SELECT uniqExact(bs), max(bs), count()
FROM
(
    SELECT blockSize() AS bs
    FROM (SELECT number % 10 AS k, rightPad(toString(number), 64, '_') AS v FROM numbers(100)) AS l
    INNER JOIN (SELECT number % 10 AS k, rightPad(toString(number), 64, '_') AS v2 FROM numbers(100)) AS r
    ON l.k = r.k
)
SETTINGS max_joined_block_size_rows = 20, max_joined_block_size_bytes = 4194304, join_algorithm = 'hash';

-- Same query with a byte budget the result does reach: with no row limit, the byte budget alone
-- must still split the output. So the block size must land strictly between one left row's worth
-- of matches (10) and the whole result (1000). `avg_bytes_per_row` is derived from container
-- capacities, so the exact size within that range is build-dependent, not a query-level invariant.
SELECT max(bs) > 10 AND max(bs) < 1000, count()
FROM
(
    SELECT blockSize() AS bs
    FROM (SELECT number % 10 AS k, rightPad(toString(number), 64, '_') AS v FROM numbers(100)) AS l
    INNER JOIN (SELECT number % 10 AS k, rightPad(toString(number), 64, '_') AS v2 FROM numbers(100)) AS r
    ON l.k = r.k
)
SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 4500, join_algorithm = 'hash';
