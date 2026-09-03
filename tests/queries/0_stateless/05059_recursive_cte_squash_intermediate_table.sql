-- Each recursive step writes its result into the intermediate (working) table, and the next step
-- reads that table back. The chunks are squashed before the write, so a step that produces many
-- small chunks does not leave many tiny blocks behind. `blockSize` evaluated while scanning the
-- working table makes the block layout of the previous step observable.

SET max_block_size = 10;

-- The third step reads 400 rows produced by the second one. With the default insert block size
-- thresholds they are squashed into a single block.
WITH RECURSIVE t AS
(
    SELECT 0 AS step, toUInt64(0) AS bs
    UNION ALL
    SELECT step + 1, bs FROM (SELECT step, blockSize() AS bs FROM t) ARRAY JOIN range(20) WHERE step < 3
)
SELECT max(bs) = 400 FROM t WHERE step = 3;

-- With the thresholds lowered to the minimum nothing is merged and the working table keeps the
-- small blocks the second step produced.
SET min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1;

WITH RECURSIVE t AS
(
    SELECT 0 AS step, toUInt64(0) AS bs
    UNION ALL
    SELECT step + 1, bs FROM (SELECT step, blockSize() AS bs FROM t) ARRAY JOIN range(20) WHERE step < 3
)
SELECT max(bs) < 400 FROM t WHERE step = 3;

-- The result of the recursion does not depend on the block layout.
SET min_insert_block_size_rows = DEFAULT, min_insert_block_size_bytes = DEFAULT;

WITH RECURSIVE t AS
(
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM t WHERE n < 1000
)
SELECT count(), sum(n) FROM t;

SET min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1;

WITH RECURSIVE t AS
(
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM t WHERE n < 1000
)
SELECT count(), sum(n) FROM t;
