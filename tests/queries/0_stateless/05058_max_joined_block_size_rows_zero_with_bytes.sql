SET max_block_size = 65536;
SET min_joined_block_size_rows = 65536;
SET min_joined_block_size_bytes = 512000;
SET enable_lazy_columns_replication = 0;
SET query_plan_join_swap_table = false;
SET max_threads = 1;
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

-- A right side of 2 rows filtered out of one 84-row granule, matching every left row: the byte budget
-- must come from the right rows the join emits (~450 bytes per output row here), not from the capacity
-- and the granule-wide `LowCardinality` dictionaries of the stored block divided by 2 rows.
DROP TABLE IF EXISTS t_05058_right;
CREATE TABLE t_05058_right (k UInt8, f LowCardinality(String), s LowCardinality(String), t String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05058_right SELECT 1, if(number IN (1, 31), 'yes', 'no'), concat('m', toString(number % 30)), toString(number) FROM numbers(84);

SELECT max(bs) > 4096, max(bs) < 131072, count()
FROM
(
    SELECT blockSize() AS bs
    FROM (SELECT 1 AS k, rightPad(toString(number), 256, 'x') AS v FROM numbers(65536)) AS l
    INNER JOIN (SELECT k, s, t FROM t_05058_right WHERE f = 'yes') AS r USING (k)
    WHERE NOT ignore(v, s, t)
)
SETTINGS max_joined_block_size_rows = 0, max_joined_block_size_bytes = 4194304, join_algorithm = 'hash';

DROP TABLE t_05058_right;

-- Two right keys copied from the same left key, which is not in the output: the output block holds
-- both copies, so the byte budget must count both of them.
SELECT max(bs * b) <= 65536, count()
FROM
(
    SELECT blockSize() AS bs, byteSize(r.k1, r.k2) AS b
    FROM (SELECT rightPad(toString(number % 2), 1024, 'x') AS k FROM numbers(4096)) AS l
    INNER JOIN
    (
        SELECT rightPad(toString(number % 2), 1024, 'x') AS k1, rightPad(toString(number % 2), 1024, 'x') AS k2
        FROM numbers(8)
    ) AS r ON l.k = r.k1 AND l.k = r.k2
)
SETTINGS max_block_size = 4096, max_joined_block_size_rows = 0, max_joined_block_size_bytes = 65536, join_algorithm = 'hash';
