-- Tags: no-fasttest, use-vectorscan
-- no-fasttest: needs vectorscan for `multiMatchAny` to reject the non-UTF-8 pattern

-- A chain of `match()` predicates where one pattern is not valid UTF-8. `match` uses RE2, which
-- accepts such patterns (matching them byte-wise / as Latin-1), but `multiMatchAny` compiles its
-- patterns with Hyperscan's `HS_FLAG_UTF8` and rejects a non-UTF-8 pattern with `BAD_ARGUMENTS`.
-- With `optimize_or_like_chain` now enabled by default, the rewrite must not turn such a
-- previously-working query into an exception: the non-UTF-8 pattern keeps the chain off
-- `multiMatchAny`, and we no longer fall back to a combined `match` alternation, so the original
-- `OR` chain is kept unchanged. Verify the chain succeeds and returns the same result as the
-- un-rewritten OR chain, for both the new and the old analyzer.

DROP TABLE IF EXISTS t_or_like_non_utf8;
CREATE TABLE t_or_like_non_utf8 (s String) ENGINE = Memory;

-- Row 1 has the bytes C3 A9 62 63 ("ébc"). The regexp byte-substring A9 62 63 (the needle '\xA9bc'
-- below) is not valid UTF-8 but byte-matches this row. The other rows match the plain-ASCII branches.
INSERT INTO t_or_like_non_utf8 VALUES (unhex('C3A96263')), ('has one here'), ('has two here'), ('has three'), ('plain');

-- For context: `multiMatchAny` itself rejects the non-UTF-8 pattern, which is exactly why the rewrite
-- must not produce it.
SELECT count() FROM t_or_like_non_utf8 WHERE multiMatchAny(s, ['\xA9bc']); -- { serverError BAD_ARGUMENTS }

-- Reference: rewrite disabled.
SELECT count() FROM t_or_like_non_utf8
WHERE match(s, '\xA9bc') OR match(s, 'one') OR match(s, 'two') OR match(s, 'three') OR match(s, 'nomatch')
SETTINGS optimize_or_like_chain = 0;

-- Rewrite enabled, new analyzer: must fall back to `match` (RE2) and return the same result.
SELECT count() FROM t_or_like_non_utf8
WHERE match(s, '\xA9bc') OR match(s, 'one') OR match(s, 'two') OR match(s, 'three') OR match(s, 'nomatch')
SETTINGS optimize_or_like_chain = 1, optimize_or_like_chain_min_patterns = 1, enable_analyzer = 1;

-- Rewrite enabled, old analyzer: same.
SELECT count() FROM t_or_like_non_utf8
WHERE match(s, '\xA9bc') OR match(s, 'one') OR match(s, 'two') OR match(s, 'three') OR match(s, 'nomatch')
SETTINGS optimize_or_like_chain = 1, optimize_or_like_chain_min_patterns = 1, enable_analyzer = 0;

DROP TABLE t_or_like_non_utf8;
