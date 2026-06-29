-- Smoke test for LLVM coverage pipeline. SELECT 1 is already covered
-- in every master run so this should NOT appear in Newly Covered output.
SELECT 1;
