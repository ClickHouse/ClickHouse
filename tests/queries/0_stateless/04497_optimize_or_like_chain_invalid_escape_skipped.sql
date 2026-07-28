-- `optimize_or_like_chain` converts every candidate `LIKE`/`ILIKE` pattern to a regexp while
-- collecting branches. `likePatternToRegexp` throws `CANNOT_PARSE_ESCAPE_SEQUENCE` for a malformed
-- pattern (e.g. a trailing backslash `'a\'`). The optimizer must not surface that error eagerly
-- during analysis: with short-circuit `or` evaluation the malformed branch may never be evaluated at
-- runtime (here `s LIKE '%'` is always true), and a below-threshold group is kept unchanged anyway.
-- A failed conversion must therefore make the rewrite keep the group's original branches, so the
-- query keeps the same runtime behavior it has with the rewrite disabled instead of failing during
-- optimization. `short_circuit_function_evaluation = 'force_enable'` makes the no-throw runtime
-- behavior deterministic.

SET short_circuit_function_evaluation = 'force_enable';

-- Eligible group (min_patterns = 1): the malformed branch fails to convert -> keep originals -> 1.
SELECT count() FROM (SELECT materialize('x') AS s)
WHERE s LIKE '%' OR s LIKE 'a\\'
SETTINGS optimize_or_like_chain = 0, optimize_or_like_chain_min_patterns = 1, enable_analyzer = 1;
SELECT count() FROM (SELECT materialize('x') AS s)
WHERE s LIKE '%' OR s LIKE 'a\\'
SETTINGS optimize_or_like_chain = 1, optimize_or_like_chain_min_patterns = 1, enable_analyzer = 1;
SELECT count() FROM (SELECT materialize('x') AS s)
WHERE s LIKE '%' OR s LIKE 'a\\'
SETTINGS optimize_or_like_chain = 1, optimize_or_like_chain_min_patterns = 1, enable_analyzer = 0;

-- Below-threshold group (default min_patterns = 5): conversion still happens during collection, so
-- the eager error must be suppressed here too -> 1.
SELECT count() FROM (SELECT materialize('x') AS s)
WHERE s LIKE '%' OR s LIKE 'a\\'
SETTINGS optimize_or_like_chain = 1, optimize_or_like_chain_min_patterns = 5, enable_analyzer = 1;
SELECT count() FROM (SELECT materialize('x') AS s)
WHERE s LIKE '%' OR s LIKE 'a\\'
SETTINGS optimize_or_like_chain = 1, optimize_or_like_chain_min_patterns = 5, enable_analyzer = 0;
