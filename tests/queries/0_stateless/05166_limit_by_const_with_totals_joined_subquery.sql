SET output_format_write_statistics = 0;

-- A `WITH TOTALS` subquery on the right side of a JOIN is fully read while the join is built.
-- LIMIT BY's constant-key early stop must not drop its totals block in the old analyzer.
SELECT l.n, r.s, 'grp' AS k
FROM
(
    SELECT number AS n
    FROM numbers(6)
) AS l
INNER JOIN
(
    SELECT number % 3 AS n, sum(number) AS s
    FROM numbers(6)
    GROUP BY n WITH TOTALS
) AS r ON l.n = r.n
LIMIT 1 BY k
LIMIT 1
FORMAT JSONCompact
SETTINGS max_threads = 1, max_block_size = 1, enable_analyzer = 0, join_algorithm = 'hash';
