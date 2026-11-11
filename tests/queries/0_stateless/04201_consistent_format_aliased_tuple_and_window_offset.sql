-- Regression tests for two `Inconsistent AST formatting` cases involving the
-- `parenthesized` flag and aliases:
--
-- 1) An aliased multi-element tuple literal wrapped in extra grouping parens
--    (e.g. `((1, 2)) AS x`) used to format-parse-format unstably because the
--    parser flattened `((1, 2))` to a tuple literal that lost the `parenthesized`
--    flag, and the formatter's `(expr) AS alias` deferral added one paren too
--    many to the output of `formatImplWithoutAlias` (which already emits `(...)`).
--
-- 2) An aliased frame offset in a window definition (e.g. `((1 + 1) AS x) PRECEDING`)
--    used to format as `(1 + 1) AS x PRECEDING`, which the parser cannot accept
--    (the alias terminates the offset expression before `PRECEDING`).
--    The window-frame formatter now emits the outer parens around an aliased
--    offset so it round-trips back to itself.

SELECT formatQuerySingleLine('SELECT (((1), 0.648) AS a7)');
SELECT formatQuerySingleLine('SELECT ((1, 2)) AS x');
SELECT formatQuerySingleLine('SELECT 1 IN (((1), (2)) AS x) FROM numbers(1)');
SELECT formatQuerySingleLine('SELECT count() OVER (ORDER BY 1 ROWS BETWEEN ((1 + 1) AS frame_offset_begin) PRECEDING AND ((2 + 2) AS frame_offset_end) FOLLOWING) FROM numbers(10)');
