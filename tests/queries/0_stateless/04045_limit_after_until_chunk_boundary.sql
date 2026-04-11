-- Tests for LIMIT AFTER/UNTIL correctness across chunk boundaries.
-- `max_block_size = 1` forces one row per chunk so every state transition
-- happens across separate transform() calls, which maximises coverage of the
-- inter-chunk bookkeeping.

SET allow_experimental_limit_after = 1;
SET max_block_size = 1;

-- UNTIL fires in an earlier chunk than AFTER.
-- Critical regression test: the old code skipped the UNTIL check when AFTER
-- was absent from the chunk, so AFTER firing in a later chunk would produce
-- rows instead of an empty result.
SELECT count() FROM (SELECT number FROM numbers(10) ORDER BY number LIMIT AFTER number = 6 UNTIL number = 2);
SELECT count() FROM (SELECT number FROM numbers(10) ORDER BY number LIMIT AFTER number = 6 UNTIL number = 2) SETTINGS enable_analyzer = 0;

-- UNTIL fires in the chunk immediately before AFTER fires.
-- `number = 5` fires at row 5, `number = 6` fires at row 6.  Window never opens.
SELECT count() FROM (SELECT number FROM numbers(10) ORDER BY number LIMIT AFTER number = 6 UNTIL number = 5);
SELECT count() FROM (SELECT number FROM numbers(10) ORDER BY number LIMIT AFTER number = 6 UNTIL number = 5) SETTINGS enable_analyzer = 0;

-- Normal window that spans multiple chunks (AFTER at row 2, UNTIL at row 5).
SELECT number FROM numbers(8) ORDER BY number LIMIT AFTER number >= 2 UNTIL number >= 5;
SELECT number FROM numbers(8) ORDER BY number LIMIT AFTER number >= 2 UNTIL number >= 5 SETTINGS enable_analyzer = 0;

-- Numeric LIMIT cap applied across chunk boundaries (cap at 2 rows after AFTER).
SELECT number FROM numbers(8) ORDER BY number LIMIT 2 AFTER number >= 2;
SELECT number FROM numbers(8) ORDER BY number LIMIT 2 AFTER number >= 2 SETTINGS enable_analyzer = 0;

-- AFTER fires at row 0 (first row of the stream).
SELECT number FROM numbers(5) ORDER BY number LIMIT AFTER number >= 0 UNTIL number >= 3;
SELECT number FROM numbers(5) ORDER BY number LIMIT AFTER number >= 0 UNTIL number >= 3 SETTINGS enable_analyzer = 0;

-- AFTER fires at the very last row of the stream: only that one row is emitted.
SELECT number FROM numbers(5) ORDER BY number LIMIT AFTER number >= 4;
SELECT number FROM numbers(5) ORDER BY number LIMIT AFTER number >= 4 SETTINGS enable_analyzer = 0;

-- UNTIL-only: output from row 0 until the condition fires.
SELECT number FROM numbers(5) ORDER BY number LIMIT UNTIL number >= 3;
SELECT number FROM numbers(5) ORDER BY number LIMIT UNTIL number >= 3 SETTINGS enable_analyzer = 0;

-- UNTIL fires at row 0: result is empty.
SELECT count() FROM (SELECT number FROM numbers(5) ORDER BY number LIMIT UNTIL number >= 0);
SELECT count() FROM (SELECT number FROM numbers(5) ORDER BY number LIMIT UNTIL number >= 0) SETTINGS enable_analyzer = 0;

-- AFTER and UNTIL fire on the same row: window is empty (UNTIL is exclusive).
SELECT count() FROM (SELECT number FROM numbers(5) ORDER BY number LIMIT AFTER number >= 3 UNTIL number >= 3);
SELECT count() FROM (SELECT number FROM numbers(5) ORDER BY number LIMIT AFTER number >= 3 UNTIL number >= 3) SETTINGS enable_analyzer = 0;

-- AFTER never fires: empty result.
SELECT count() FROM (SELECT number FROM numbers(5) ORDER BY number LIMIT AFTER number >= 10);
SELECT count() FROM (SELECT number FROM numbers(5) ORDER BY number LIMIT AFTER number >= 10) SETTINGS enable_analyzer = 0;

-- ALL mode: windows span multiple chunks; rows_read accumulates correctly.
SELECT number FROM numbers(10) ORDER BY number LIMIT 2 AFTER number IN (2, 6) ALL;
SELECT number FROM numbers(10) ORDER BY number LIMIT 2 AFTER number IN (2, 6) ALL SETTINGS enable_analyzer = 0;

-- ALL mode with UNTIL: each AFTER opens a window closed by the first UNTIL.
SELECT number FROM numbers(10) ORDER BY number LIMIT AFTER number IN (3, 7) ALL UNTIL number IN (1, 5, 9);
SELECT number FROM numbers(10) ORDER BY number LIMIT AFTER number IN (3, 7) ALL UNTIL number IN (1, 5, 9) SETTINGS enable_analyzer = 0;
