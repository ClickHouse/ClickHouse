-- Test that GenerateRandom engine rejects non-literal arguments with a clear error message
-- This is a regression test for the bug where random() function call caused a bad cast error

CREATE TABLE t0 (c0 Int32) ENGINE = GenerateRandom(random()); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t1 (c0 Int32) ENGINE = GenerateRandom(now()); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t2 (c0 Int32) ENGINE = GenerateRandom(1 + 1); -- { serverError BAD_ARGUMENTS }

-- Valid literal arguments should still work
CREATE TABLE t3 (c0 Int32) ENGINE = GenerateRandom(42);
DROP TABLE t3;

CREATE TABLE t4 (c0 Int32) ENGINE = GenerateRandom(100, 20, 30);
DROP TABLE t4;
