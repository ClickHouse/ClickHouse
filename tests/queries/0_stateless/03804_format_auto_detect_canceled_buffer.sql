-- Test that format auto-detection works correctly when some formats fail during schema inference.
-- This is a regression test for a bug where the ReadBuffer was marked as canceled after one format
-- failed, causing subsequent format detection attempts to fail with "ReadBuffer is canceled. Can't read from it."

SELECT * FROM format('{"a": 1}'); -- JSONEachRow should be auto-detected

-- The original query from AST fuzzer that triggered the bug (simplified)
-- The issue occurred because TSKV format detection would fail, cancel the buffer, and then
-- subsequent format detection attempts would fail.
SELECT * FROM format('1,2,3'); -- CSV should be auto-detected
