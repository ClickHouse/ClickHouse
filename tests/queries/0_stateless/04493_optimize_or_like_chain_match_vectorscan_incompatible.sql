-- Tags: no-fasttest, use-vectorscan
-- no-fasttest: needs vectorscan for `multiMatchAny` to reject the RE2-only pattern

-- A chain of `match()` predicates where one pattern uses RE2 syntax that Vectorscan rejects even
-- though its bytes are valid UTF-8. `\C` ("match any byte") is accepted by RE2 (used by `match`) but
-- rejected by Vectorscan under `HS_FLAG_UTF8` with `BAD_ARGUMENTS` ("\C is unsupported in UTF8").
-- The `allRegexpsValidUTF8` guard does not catch this (the pattern bytes `\` and `C` are valid
-- UTF-8). With `optimize_or_like_chain` now enabled by default, the rewrite must not turn such a
-- previously-working query into an exception: a chain that contains a raw `match()` regexp is kept
-- off `multiMatchAny`, and we no longer fall back to a combined `match` alternation, so the original
-- `OR` chain is kept unchanged. Verify the chain succeeds and returns the same result as the
-- un-rewritten OR chain, for both the new and the old analyzer.

DROP TABLE IF EXISTS t_or_like_match_re2;
CREATE TABLE t_or_like_match_re2 (s String) ENGINE = Memory;
INSERT INTO t_or_like_match_re2 VALUES ('xCy'), ('one'), ('two'), ('three'), ('plain');

-- For context: `multiMatchAny` itself rejects the `\C` pattern, which is exactly why the rewrite
-- must not produce it.
SELECT count() FROM t_or_like_match_re2 WHERE multiMatchAny(s, ['\\C']); -- { serverError BAD_ARGUMENTS }

-- Reference: rewrite disabled. `\C` matches every (non-empty) row -> 5.
SELECT count() FROM t_or_like_match_re2
WHERE match(s, '\\C') OR match(s, 'one') OR match(s, 'two') OR match(s, 'three') OR match(s, 'nomatch')
SETTINGS optimize_or_like_chain = 0;

-- Rewrite enabled, new analyzer: must fall back to combined `match` (RE2) and return the same result.
SELECT count() FROM t_or_like_match_re2
WHERE match(s, '\\C') OR match(s, 'one') OR match(s, 'two') OR match(s, 'three') OR match(s, 'nomatch')
SETTINGS optimize_or_like_chain = 1, optimize_or_like_chain_min_patterns = 1, allow_hyperscan = 1, enable_analyzer = 1;

-- Rewrite enabled, old analyzer: same.
SELECT count() FROM t_or_like_match_re2
WHERE match(s, '\\C') OR match(s, 'one') OR match(s, 'two') OR match(s, 'three') OR match(s, 'nomatch')
SETTINGS optimize_or_like_chain = 1, optimize_or_like_chain_min_patterns = 1, allow_hyperscan = 1, enable_analyzer = 0;

-- Same chain at the default thresholds (5 patterns is below `optimize_or_like_chain_min_patterns`, so
-- the chain is kept as-is), to prove the default-on rewrite no longer turns this into an exception.
SELECT count() FROM t_or_like_match_re2
WHERE match(s, '\\C') OR match(s, 'one') OR match(s, 'two') OR match(s, 'three') OR match(s, 'nomatch')
SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT count() FROM t_or_like_match_re2
WHERE match(s, '\\C') OR match(s, 'one') OR match(s, 'two') OR match(s, 'three') OR match(s, 'nomatch')
SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 0;

DROP TABLE t_or_like_match_re2;
