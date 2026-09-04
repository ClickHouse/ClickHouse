SET enable_analyzer = 1;

-- Each recursive step writes its result into the intermediate (working) table, and the next step
-- reads that table back. The chunks are squashed before the write, so a step that produces many
-- small chunks does not leave many tiny blocks behind. `blockSize` evaluated while scanning the
-- working table makes the block layout of the previous step observable.

-- The anchor produces three chunks of 2, 2 and 1 rows, all smaller than `max_block_size`. Without
-- squashing each of them becomes a separate block of the working table; with squashing the first
-- recursive step reads them as a single block of 5 rows.
SET max_block_size = 10;

WITH RECURSIVE t AS
(
    SELECT number AS n, toUInt64(0) AS bs FROM numbers(25) WHERE number % 5 = 0
    UNION ALL
    SELECT n, blockSize() FROM t WHERE bs = 0
)
SELECT uniqExact(bs), max(bs) FROM t WHERE bs > 0;

SET max_block_size = DEFAULT;

-- The upper bounds follow the `INSERT` settings too: with strict limits a chunk larger than
-- `max_insert_block_size` is split before it is written, here 20 rows into blocks of 8, 8 and 4.
SET max_block_size = 8, max_insert_block_size = 8, use_strict_insert_block_limits = 1;

WITH RECURSIVE t AS
(
    SELECT 0 AS step, toUInt64(0) AS bs FROM (SELECT 1) ARRAY JOIN range(20) AS i
    UNION ALL
    SELECT step + 1, blockSize() FROM t WHERE step = 0
)
SELECT max(bs), uniqExact(bs) FROM t WHERE step = 1;

SET max_block_size = DEFAULT, max_insert_block_size = DEFAULT, use_strict_insert_block_limits = DEFAULT;

-- The following two cases are invariance checks rather than regression coverage: `ARRAY JOIN` already
-- emits the whole expansion of a block as one chunk unless it exceeds `max_block_size`, so they hold
-- without squashing as well.

-- The third step reads the 400 rows produced by the second one as a single block.
WITH RECURSIVE t AS
(
    SELECT 0 AS step, toUInt64(0) AS bs
    UNION ALL
    SELECT step + 1, bs FROM (SELECT step, blockSize() AS bs FROM t) ARRAY JOIN range(20) WHERE step < 3
)
SELECT max(bs) = 400, uniqExact(bs) = 1 FROM t WHERE step = 3;

-- The intermediate table is a `Memory` table, which prefers smaller blocks, so the squashing
-- accumulates only up to `max_block_size` rows before flushing rather than up to the insert thresholds.
SET max_block_size = 10;

WITH RECURSIVE t AS
(
    SELECT 0 AS step, toUInt64(0) AS bs
    UNION ALL
    SELECT step + 1, bs FROM (SELECT step, blockSize() AS bs FROM t) ARRAY JOIN range(20) WHERE step < 3
)
SELECT max(bs) < 400 FROM t WHERE step = 3;

-- A recursive member with `WITH TOTALS` has a totals stream, which the intermediate table sink drops;
-- the squashing must not be attached to it (found by the AST fuzzer).
WITH RECURSIVE t AS
(
    SELECT 65537 AS n
    UNION ALL
    SELECT intDivOrZero(toInt128(1024), n) FROM t WHERE toLowCardinality(9223372036854775806) >= n AND n > 0 GROUP BY ALL WITH TOTALS
)
SELECT count() FROM t;

-- The extremes stream is dropped by the sink in the same way as the totals stream.
SET extremes = 1;

WITH RECURSIVE t AS
(
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM t WHERE n < 5
)
SELECT count() FROM t FORMAT Null;

SET extremes = DEFAULT;

-- The result of the recursion does not depend on the block layout. Every recursive step re-plans and
-- re-executes the member, which is slow in sanitizer builds, so keep the depth moderate.
SET max_block_size = DEFAULT;

WITH RECURSIVE t AS
(
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM t WHERE n < 100
)
SELECT count(), sum(n) FROM t;

SET max_block_size = 7;

WITH RECURSIVE t AS
(
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM t WHERE n < 100
)
SELECT count(), sum(n) FROM t;
