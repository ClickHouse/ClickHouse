-- A left semi join keeps only the left rows that have a match, so an empty right side makes the whole
-- result empty and the left side does not have to be read at all.
-- The right side is empty only at runtime, and the join runtime filter is switched off, because it stops
-- the left side on its own and would hide a regression here.

-- The CI test config caps the rows a query may read, and the left side declares more than that even
-- though the join never reads it.
SET max_rows_to_read = 0, max_bytes_to_read = 0;
SET enable_join_runtime_filters = 0;
-- Keep the empty side on the build side: a swap makes the 50 million rows the build side, which the join
-- then has to read before it can know that the result is empty.
SET query_plan_join_swap_table = 'false';

SELECT count()
FROM
(
    SELECT number FROM numbers(50000000)
) AS l
SEMI LEFT JOIN
(
    SELECT number FROM numbers(1000) WHERE sipHash64(number) = 42
) AS r ON l.number = r.number
SETTINGS join_algorithm = 'hash', log_comment = '05154_hash';

SELECT count()
FROM
(
    SELECT number FROM numbers(50000000)
) AS l
SEMI LEFT JOIN
(
    SELECT number FROM numbers(1000) WHERE sipHash64(number) = 42
) AS r ON l.number = r.number
SETTINGS join_algorithm = 'parallel_hash', log_comment = '05154_parallel_hash';

SELECT count()
FROM
(
    SELECT number FROM numbers(50000000)
) AS l
SEMI LEFT JOIN
(
    SELECT number FROM numbers(1000) WHERE sipHash64(number) = 42
) AS r ON l.number = r.number
SETTINGS join_algorithm = 'grace_hash', log_comment = '05154_grace_hash';

SYSTEM FLUSH LOGS query_log;

-- The left side holds 50 million rows and only the 1000 rows of the right side may be read.
SELECT log_comment, read_rows < 1000000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment IN ('05154_hash', '05154_parallel_hash', '05154_grace_hash')
ORDER BY log_comment;

SELECT 'a non-empty right side is unaffected';
SELECT count() FROM (SELECT number FROM numbers(10)) AS l SEMI LEFT JOIN (SELECT number FROM numbers(5)) AS r ON l.number = r.number;

SELECT 'an anti join keeps the left rows an empty right side leaves unmatched';
SELECT count() FROM (SELECT number FROM numbers(10)) AS l ANTI LEFT JOIN (SELECT number FROM numbers(1000) WHERE sipHash64(number) = 42) AS r ON l.number = r.number;

SELECT 'and so does a left join';
SELECT count() FROM (SELECT number FROM numbers(10)) AS l LEFT JOIN (SELECT number FROM numbers(1000) WHERE sipHash64(number) = 42) AS r ON l.number = r.number;
