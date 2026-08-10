-- `optimize_or_like_chain` must preserve results for a `LIKE`/`ILIKE` pattern whose generated regexp
-- contains an embedded NUL byte and is NOT a pure substring (so it goes to the regexp rewrite path,
-- not the byte-oriented `multiSearchAny*`). `multiMatchAny` compiles each pattern through a
-- NUL-terminated Vectorscan API and truncates it at the first NUL, whereas the original `like`/`ilike`
-- compiles the full pattern with length-aware RE2. A pattern such as `s LIKE 'a\0_'` (regexp `^a\x00.`)
-- would therefore match a *broader* set under a truncated `multiMatchAny` (`^a`) than under the
-- original predicate. The rewrite must keep such chains off `multiMatchAny`, and we no longer fall
-- back to a combined `match` alternation, so the original branches are kept. The optimized result
-- must equal the unoptimized one regardless of `allow_hyperscan` and the analyzer.

SET optimize_or_like_chain_min_patterns = 1;

DROP TABLE IF EXISTS t_or_like_nul_derived;
CREATE TABLE t_or_like_nul_derived (s String) ENGINE = Memory;
-- 'aXb' matches the NUL-truncated form '^a' but NOT the full '^a\x00.'; 'a\0b' matches both.
INSERT INTO t_or_like_nul_derived VALUES ('aXb'), ('a\0b'), ('cd');

-- Baseline (rewrite disabled): only 'a\0b' and 'cd' match -> 2.
SELECT count() FROM t_or_like_nul_derived WHERE s LIKE 'a\0_' OR s LIKE 'cd' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;

-- Rewrite enabled with Hyperscan allowed: must still give 2 (must not take the truncating `multiMatchAny`).
SELECT count() FROM t_or_like_nul_derived WHERE s LIKE 'a\0_' OR s LIKE 'cd' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT count() FROM t_or_like_nul_derived WHERE s LIKE 'a\0_' OR s LIKE 'cd' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 0;

-- Rewrite enabled with Hyperscan disabled (no fast path applies -> keep originals): 2.
SELECT count() FROM t_or_like_nul_derived WHERE s LIKE 'a\0_' OR s LIKE 'cd' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;
SELECT count() FROM t_or_like_nul_derived WHERE s LIKE 'a\0_' OR s LIKE 'cd' SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 0;

DROP TABLE t_or_like_nul_derived;
