-- Tags: long
-- Regression test: the JIT-compiled regexp matcher must run in linear (not quadratic) time on the
-- shapes flagged in review. A quadratic implementation rescans the whole run either at every start
-- offset (leading unbounded quantifier on rejected input) or on every iterative match (bounded
-- quantifier called from `extractAll` / `replaceRegexpAll`), and would time out on the large inputs
-- below; the linear implementation returns quickly. `min_count_to_compile_regular_expression = 0`
-- forces the JIT path on first use, and results must match the general RE2 engine.

SET compile_regular_expressions = 1;
SET min_count_to_compile_regular_expression = 0;

SELECT '-- correctness parity with RE2 on the affected shapes';
SELECT match(concat(repeat('1', 137), 'b'), '[0-9]+a');
SELECT match(concat(repeat('1', 137), 'a'), '[0-9]+a');
SELECT match(concat(repeat('1', 137), 'a'), '([0-9]+)a');
SELECT extractAll(repeat('1', 10), '[0-9]{1,3}');
SELECT replaceRegexpAll('11119', '[0-9]{1,3}', 'x');

-- A leading unbounded quantifier can also sit behind a zero-width optional (`()?`, `(?:)?`), which
-- must not defeat the resume-past-run skip.
SELECT match(concat(repeat('1', 137), 'b'), '()?[0-9]+a');
SELECT match(concat(repeat('1', 137), 'a'), '()?[0-9]+a');
SELECT match(concat(repeat('1', 137), 'a'), '(?:)?[0-9]+a');

-- `repeat` caps its count at 1000000, so nest it to build a 4000000-byte run.
SELECT '-- leading unbounded quantifier, rejected input: resumes past the run instead of retrying every offset';
SELECT match(concat(repeat(repeat('1', 1000000), 4), 'b'), '[0-9]+a');
SELECT match(concat(repeat(repeat('1', 1000000), 4), 'b'), '()?[0-9]+a');
SELECT match(concat(repeat(repeat('1', 1000000), 4), 'b'), '(?:)?[0-9]+a');

SELECT '-- bounded quantifier via iterative callers: scans at most `max` bytes per match';
SELECT length(extractAll(repeat(repeat('1', 1000000), 4), '[0-9]{1,3}'));
SELECT length(replaceRegexpAll(repeat(repeat('1', 1000000), 4), '[0-9]{1,3}', 'x'));
