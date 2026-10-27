-- Tags: no-old-analyzer

-- The IEJoin operator materializes both inputs entirely, so `max_rows_in_join` /
-- `max_bytes_in_join` apply to the total accumulated input.

SET join_algorithm = 'direct,parallel_hash,hash,ie_join';

DROP TABLE IF EXISTS lim_l;
DROP TABLE IF EXISTS lim_r;

CREATE TABLE lim_l (id Int32, x Int32, y Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE lim_r (id Int32, x Int32, y Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO lim_l SELECT number, number % 10, number % 7 FROM numbers(100);
INSERT INTO lim_r SELECT number, number % 9, number % 8 FROM numbers(100);

SELECT 'plan', count() > 0 FROM (EXPLAIN SELECT count() FROM lim_l l JOIN lim_r r ON l.x < r.x AND l.y > r.y) WHERE explain LIKE '%IEJoin%';

-- A limit above the input size does not fire.
SET max_rows_in_join = 1000;
SELECT 'under limit', count() FROM lim_l l JOIN lim_r r ON l.x < r.x AND l.y > r.y;

SET max_rows_in_join = 50;
SELECT count() FROM lim_l l JOIN lim_r r ON l.x < r.x AND l.y > r.y; -- { serverError SET_SIZE_LIMIT_EXCEEDED }

-- 'break' keeps the input read so far, including the chunk that reached the limit, and stops
-- reading; a single chunk per side covers the whole input here, so the result stays complete.
SET join_overflow_mode = 'break';
SELECT 'break', count() FROM lim_l l JOIN lim_r r ON l.x < r.x AND l.y > r.y;

-- The chunk that reaches the limit is kept even when it is the first one, so the truncated input
-- is not empty (LEFT emits every row of it, matched or padded).
SELECT 'break, first chunk over limit', count() > 0 FROM lim_l l LEFT JOIN lim_r r ON l.x < r.x AND l.y > r.y
SETTINGS max_block_size = 10, max_rows_in_join = 5;

SET max_rows_in_join = 0;
SET join_overflow_mode = 'throw';
SET max_bytes_in_join = 100;
SELECT count() FROM lim_l l JOIN lim_r r ON l.x < r.x AND l.y > r.y; -- { serverError SET_SIZE_LIMIT_EXCEEDED }
SET max_bytes_in_join = 0;

-- Result blocks are bounded by `max_joined_block_size_rows`, as with the other algorithms.
SELECT 'result block size', max(rows) <= 100, max(rows) > 0
FROM (SELECT blockSize() AS rows FROM lim_l l JOIN lim_r r ON l.x < r.x AND l.y > r.y SETTINGS max_joined_block_size_rows = 100);

DROP TABLE lim_l;
DROP TABLE lim_r;
