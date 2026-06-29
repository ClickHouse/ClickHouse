-- `optimize_or_like_chain` must preserve results for `OR` chains whose regexp contains an embedded
-- NUL byte. RE2's required-substring optimization truncates a lone `match`/`LIKE` pattern at the
-- first NUL (so the original `match(s, 'a\0b')` already behaves like `match(s, 'a')`), but the
-- combined `(p1)|(p2)|...` alternation that the rewrite would otherwise build is NOT truncated and
-- would match a narrower set. The rewrite therefore uses `multiMatchAny` (which truncates at NUL
-- exactly like the original per-branch `match`) when Hyperscan is allowed, and keeps the original
-- branches when it would have to fall back to the combined `match`. Either way the result must equal
-- the unoptimized chain, for both analyzers and regardless of `allow_hyperscan`.

SET optimize_or_like_chain_min_patterns = 1;

DROP TABLE IF EXISTS t_or_like_chain_nul;
CREATE TABLE t_or_like_chain_nul (s String) ENGINE = Memory;
-- 'xay' contains 'a' (the truncated form of the NUL pattern) but not the byte sequence "a\0b".
INSERT INTO t_or_like_chain_nul VALUES ('xay'), ('a\0bd'), ('cd');

-- Baselines with the rewrite disabled. `match` truncates "a\0b" to "a", so 'xay' matches too -> 3.
SELECT count() FROM t_or_like_chain_nul WHERE match(s, 'a\0b') OR match(s, 'cd') SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
-- The `LIKE` substring path is byte-oriented (not truncated), so 'xay' does not match -> 2.
SELECT count() FROM t_or_like_chain_nul WHERE s LIKE '%a\0b%' OR s LIKE '%cd%' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;

-- `match` chain rewritten with Hyperscan allowed (-> multiMatchAny) must still give 3.
SELECT count() FROM t_or_like_chain_nul WHERE match(s, 'a\0b') OR match(s, 'cd') SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 1;
SELECT count() FROM t_or_like_chain_nul WHERE match(s, 'a\0b') OR match(s, 'cd') SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 1, enable_analyzer = 0;
-- `match` chain rewritten with Hyperscan disabled (-> keep originals, not combined match) must give 3.
SELECT count() FROM t_or_like_chain_nul WHERE match(s, 'a\0b') OR match(s, 'cd') SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 1;
SELECT count() FROM t_or_like_chain_nul WHERE match(s, 'a\0b') OR match(s, 'cd') SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0, enable_analyzer = 0;

-- `LIKE` substring chain rewritten (-> byte-oriented multiSearchAny) must still give 2.
SELECT count() FROM t_or_like_chain_nul WHERE s LIKE '%a\0b%' OR s LIKE '%cd%' SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;
SELECT count() FROM t_or_like_chain_nul WHERE s LIKE '%a\0b%' OR s LIKE '%cd%' SETTINGS optimize_or_like_chain = 1, enable_analyzer = 0;

DROP TABLE t_or_like_chain_nul;
