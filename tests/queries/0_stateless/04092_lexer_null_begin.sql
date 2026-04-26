-- Smoke test for `formatQueryOrNull` on an empty input.
-- The `nullptr` begin pointer path that triggered UB in `Lexer::nextToken`
-- is covered directly by `gtest_Lexer.cpp` (`Lexer.NullBuffer`); here `begin`
-- is a non-null empty-buffer pointer from `ColumnString` storage.

SELECT formatQueryOrNull('');
