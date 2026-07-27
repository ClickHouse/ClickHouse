-- `optimize_or_like_chain` must preserve results for `OR` chains whose regexp contains an embedded
-- NUL byte. `match`/`LIKE` are byte-oriented: RE2 matches `\0` literally (see the fix in commit
-- 2655ec4ea51), so `match(s, 'a\0b')` matches only strings containing the byte sequence "a\0b", not
-- the truncated "a". The rewrite must reproduce this. `multiMatchAny` compiles each pattern through a
-- NUL-terminated Vectorscan API and would truncate "a\0b" to "a" (matching a broader set), so the
-- rewrite keeps the original branches for `match`/regexp chains that contain an embedded NUL (it does
-- not fall back to a combined `match` alternation). The byte-oriented `multiSearchAny*` substring path
-- used for `LIKE` chains is length-aware and preserving, so it stays rewritten. Either way the result
-- must equal the unoptimized chain, for both analyzers and regardless of `allow_hyperscan`.

SET optimize_or_like_chain_min_patterns = 1;
SET optimize_or_like_chain_min_substrings = 1;

DROP TABLE IF EXISTS t_or_like_chain_nul;
CREATE TABLE t_or_like_chain_nul (s String) ENGINE = Memory;
-- 'xay' contains 'a' but not the byte sequence "a\0b"; 'a\0bd' contains "a\0b".
INSERT INTO t_or_like_chain_nul VALUES ('xay'), ('a\0bd'), ('cd');

-- Baselines with the rewrite disabled. `match` is byte-oriented, so 'a\0b' matches only 'a\0bd' -> 2.
SELECT count() FROM t_or_like_chain_nul WHERE match(s, 'a\0b') OR match(s, 'cd') SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
-- The `LIKE` substring path is byte-oriented too, so 'xay' does not match -> 2.
SELECT count() FROM t_or_like_chain_nul WHERE s LIKE '%a\0b%' OR s LIKE '%cd%' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;

-- `match` chain, Hyperscan allowed: the embedded NUL keeps it off truncating `multiMatchAny`, so originals are kept -> 2.
SELECT count() FROM t_or_like_chain_nul WHERE match(s, 'a\0b') OR match(s, 'cd') SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT count() FROM t_or_like_chain_nul WHERE match(s, 'a\0b') OR match(s, 'cd') SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 0;
-- `match` chain, Hyperscan disabled: no fast path applies, so the original branches are kept -> 2.
SELECT count() FROM t_or_like_chain_nul WHERE match(s, 'a\0b') OR match(s, 'cd') SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;
SELECT count() FROM t_or_like_chain_nul WHERE match(s, 'a\0b') OR match(s, 'cd') SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 0;

-- `LIKE` substring chain rewritten (-> byte-oriented multiSearchAny) must still give 2.
SELECT count() FROM t_or_like_chain_nul WHERE s LIKE '%a\0b%' OR s LIKE '%cd%' SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;
SELECT count() FROM t_or_like_chain_nul WHERE s LIKE '%a\0b%' OR s LIKE '%cd%' SETTINGS optimize_or_like_chain = 1, enable_analyzer = 0;

DROP TABLE t_or_like_chain_nul;
