-- Regression test: Lexer with null begin pointer and non-zero max_query_size
-- used to trigger UB (adding non-zero offset to null pointer).
-- formatQueryOrNull internally creates a Lexer via parseQuery with max_query_size from settings.

-- Empty string: exercises empty-buffer Lexer path
SELECT formatQueryOrNull('');
