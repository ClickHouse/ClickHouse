-- Tags: no-fasttest
-- no-fasttest: Reference output uses `multiMatchAny`, which requires vectorscan
SET allow_hyperscan = 1, max_hyperscan_regexp_length = 0, max_hyperscan_regexp_total_length = 0;
SET optimize_rewrite_like_perfect_affix = 0; -- prevent input/output interference from another LIKE rewrite pass
-- Exercise the rewrite for the short chains in this test; production defaults are 10 (regexp) and 4 (substring).
SET optimize_or_like_chain_min_patterns = 1;
SET optimize_or_like_chain_min_substrings = 1;

EXPLAIN SYNTAX SELECT materialize('Привет, World') AS s WHERE (s LIKE 'hell%') OR (s ILIKE '%привет%') OR (s ILIKE 'world%') SETTINGS optimize_or_like_chain = 0;
EXPLAIN QUERY TREE run_passes=1 SELECT materialize('Привет, World') AS s WHERE (s LIKE 'hell%') OR (s ILIKE '%привет%') OR (s ILIKE 'world%') SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
EXPLAIN SYNTAX SELECT materialize('Привет, World') AS s WHERE (s LIKE 'hell%') OR (s ILIKE '%привет%') OR (s ILIKE 'world%') SETTINGS optimize_or_like_chain = 1;
EXPLAIN QUERY TREE run_passes=1 SELECT materialize('Привет, World') AS s WHERE (s LIKE 'hell%') OR (s ILIKE '%привет%') OR (s ILIKE 'world%') SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;

EXPLAIN SYNTAX SELECT materialize('Привет, World') AS s1, materialize('Привет, World') AS s2 WHERE (s1 LIKE 'hell%') OR (s2 ILIKE '%привет%') OR (s1 ILIKE 'world%') SETTINGS optimize_or_like_chain = 1;
EXPLAIN SYNTAX SELECT materialize('Привет, World') AS s1, materialize('Привет, World') AS s2 WHERE (s1 LIKE 'hell%') OR (s2 ILIKE '%привет%') OR (s1 ILIKE 'world%') SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0;
EXPLAIN SYNTAX SELECT materialize('Привет, World') AS s1, materialize('Привет, World') AS s2 WHERE (s1 LIKE 'hell%') OR (s2 ILIKE '%привет%') OR (s1 ILIKE 'world%') SETTINGS optimize_or_like_chain = 1, max_hyperscan_regexp_length = 5;
EXPLAIN SYNTAX SELECT materialize('Привет, World') AS s1, materialize('Привет, World') AS s2 WHERE (s1 LIKE 'hell%') OR (s2 ILIKE '%привет%') OR (s1 ILIKE 'world%') SETTINGS optimize_or_like_chain = 1, max_hyperscan_regexp_total_length = 10;
EXPLAIN SYNTAX SELECT materialize('Привет, World') AS s1, materialize('Привет, World') AS s2 WHERE (s1 LIKE 'hell%') OR (s2 ILIKE '%привет%') OR (s1 ILIKE 'world%') OR s1 == 'Привет' SETTINGS optimize_or_like_chain = 1;


SELECT materialize('Привет, optimized World') AS s WHERE (s LIKE 'hell%') OR (s LIKE '%привет%') OR (s ILIKE '%world') SETTINGS optimize_or_like_chain = 1;
SELECT materialize('Привет, optimized World') AS s WHERE (s LIKE 'hell%') OR (s LIKE '%привет%') OR (s ILIKE '%world') SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;

SELECT materialize('Привет, World') AS s WHERE (s LIKE 'hell%') OR (s LIKE '%привет%') OR (s ILIKE '%world') SETTINGS optimize_or_like_chain = 0;
SELECT materialize('Привет, World') AS s WHERE (s LIKE 'hell%') OR (s LIKE '%привет%') OR (s ILIKE '%world') SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;

SELECT materialize('Привет, optimized World') AS s WHERE (s LIKE 'hell%') OR (s ILIKE '%привет%') OR (s LIKE 'world%') SETTINGS optimize_or_like_chain = 1;
SELECT materialize('Привет, optimized World') AS s WHERE (s LIKE 'hell%') OR (s ILIKE '%привет%') OR (s LIKE 'world%') SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;

SELECT materialize('Привет, World') AS s WHERE (s LIKE 'hell%') OR (s ILIKE '%привет%') OR (s LIKE 'world%') SETTINGS optimize_or_like_chain = 0;
SELECT materialize('Привет, World') AS s WHERE (s LIKE 'hell%') OR (s ILIKE '%привет%') OR (s LIKE 'world%') SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;

SELECT materialize('Привет, World') AS s WHERE (s LIKE 'hell%') OR (s ILIKE '%привет%') OR (s ILIKE 'world%') SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;

-- Aliases

EXPLAIN SYNTAX SELECT test, materialize('Привет, World') AS s WHERE ((s LIKE 'hell%') AS test) OR (s ILIKE '%привет%') OR (s ILIKE 'world%') SETTINGS optimize_or_like_chain = 1;

-- Test `match` function combined with `LIKE`. A raw `match()` regexp is not eligible for `multiMatchAny`
-- (Vectorscan can reject RE2-only syntax), and we no longer emit a combined `match` alternation, so this
-- chain is kept as the original `OR`.
EXPLAIN SYNTAX SELECT materialize('Hello World') AS s WHERE (s LIKE 'hello%') OR match(s, 'wor.*') SETTINGS optimize_or_like_chain = 1;
EXPLAIN QUERY TREE run_passes=1 SELECT materialize('Hello World') AS s WHERE (s LIKE 'hello%') OR match(s, 'wor.*') SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;

-- Verify match() combined with LIKE returns correct results
SELECT materialize('Hello World') AS s WHERE (s LIKE 'hello%') OR match(s, 'wor.*') SETTINGS optimize_or_like_chain = 1;
SELECT materialize('Hello World') AS s WHERE (s LIKE 'hello%') OR match(s, 'wor.*') SETTINGS optimize_or_like_chain = 0;

-- Test multiple match() functions
EXPLAIN SYNTAX SELECT materialize('test string') AS s WHERE match(s, '^test') OR match(s, 'ing$') SETTINGS optimize_or_like_chain = 1;
SELECT materialize('test string') AS s WHERE match(s, '^test') OR match(s, 'ing$') SETTINGS optimize_or_like_chain = 1;

-- Test pure substring patterns (should use `multiSearchAny`)
EXPLAIN SYNTAX SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s LIKE '%World%') SETTINGS optimize_or_like_chain = 1;
EXPLAIN QUERY TREE run_passes=1 SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s LIKE '%World%') SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;

-- Verify multiSearchAny returns correct results
SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s LIKE '%World%') SETTINGS optimize_or_like_chain = 1;
SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s LIKE '%World%') SETTINGS optimize_or_like_chain = 0;

-- Test that no match still returns empty
SELECT materialize('test') AS s WHERE (s LIKE '%Hello%') OR (s LIKE '%World%') SETTINGS optimize_or_like_chain = 1;

-- Test case-insensitive substring patterns (should use multiSearchAnyCaseInsensitiveUTF8)
EXPLAIN SYNTAX SELECT materialize('Hello World') AS s WHERE (s ILIKE '%hello%') OR (s ILIKE '%world%') SETTINGS optimize_or_like_chain = 1;
EXPLAIN QUERY TREE run_passes=1 SELECT materialize('Hello World') AS s WHERE (s ILIKE '%hello%') OR (s ILIKE '%world%') SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;

-- Verify case-insensitive multiSearchAny returns correct results
SELECT materialize('Hello World') AS s WHERE (s ILIKE '%hello%') OR (s ILIKE '%world%') SETTINGS optimize_or_like_chain = 1;
SELECT materialize('Hello World') AS s WHERE (s ILIKE '%hello%') OR (s ILIKE '%world%') SETTINGS optimize_or_like_chain = 0;

-- Test mixed case sensitivity (uses `multiMatchAny` with case flags inside the regexps)
EXPLAIN SYNTAX SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s ILIKE '%world%') SETTINGS optimize_or_like_chain = 1;
EXPLAIN QUERY TREE run_passes=1 SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s ILIKE '%world%') SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;

-- Verify mixed case still returns correct results
SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s ILIKE '%world%') SETTINGS optimize_or_like_chain = 1;
SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s ILIKE '%world%') SETTINGS optimize_or_like_chain = 0;

-- Test substring patterns with non-substring patterns (uses `multiMatchAny` when `allow_hyperscan` is on)
EXPLAIN SYNTAX SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s LIKE 'World%') SETTINGS optimize_or_like_chain = 1;

-- Verify mixed pattern types still returns correct results
SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s LIKE 'World%') SETTINGS optimize_or_like_chain = 1;
SELECT materialize('World Hello') AS s WHERE (s LIKE '%Hello%') OR (s LIKE 'World%') SETTINGS optimize_or_like_chain = 1;

-- Test match() with case-insensitive regexp
EXPLAIN SYNTAX SELECT materialize('Hello World') AS s WHERE match(s, '(?i)hello') OR match(s, '(?i)world') SETTINGS optimize_or_like_chain = 1;
SELECT materialize('Hello World') AS s WHERE match(s, '(?i)hello') OR match(s, '(?i)world') SETTINGS optimize_or_like_chain = 1;

-- Mixed OR chain (LIKE + non-LIKE branch) must NOT be wrapped in indexHint, since
-- `indexHint(LIKE) AND (optimized_OR)` would prune ranges where only the non-LIKE branch matches,
-- producing false negatives. The QUERY TREE must keep just `or(...)` at the top, not `and(...)`.
EXPLAIN QUERY TREE run_passes=1 SELECT materialize('Hello World') AS s, materialize(1::UInt8) AS n WHERE (s LIKE '%Hello%') OR (n = 2) SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;

-- When per-pattern or total pattern length exceeds the hyperscan limits, the `multiMatchAny`
-- rewrite is skipped for that LHS group (it would throw at runtime); we do not fall back to a
-- combined `match` alternation, so the non-substring `s1` group keeps its original LIKE/ILIKE
-- branches, while the substring `s2` group remains rewritten as `multiSearchAny`.
EXPLAIN QUERY TREE run_passes=1 SELECT materialize('Привет, World') AS s1, materialize('Привет, World') AS s2 WHERE (s1 LIKE 'hell%') OR (s2 ILIKE '%привет%') OR (s1 ILIKE 'world%') SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1, max_hyperscan_regexp_length = 5;
EXPLAIN QUERY TREE run_passes=1 SELECT materialize('Привет, World') AS s1, materialize('Привет, World') AS s2 WHERE (s1 LIKE 'hell%') OR (s2 ILIKE '%привет%') OR (s1 ILIKE 'world%') SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1, max_hyperscan_regexp_total_length = 10;

-- Verify results stay correct when the size guard skips the rewrite (the row should still match).
SELECT materialize('Привет, World') AS s1, materialize('Привет, World') AS s2 WHERE (s1 LIKE 'hell%') OR (s2 ILIKE '%привет%') OR (s1 ILIKE 'world%') SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1, max_hyperscan_regexp_length = 5;
SELECT materialize('Привет, World') AS s1, materialize('Привет, World') AS s2 WHERE (s1 LIKE 'hell%') OR (s2 ILIKE '%привет%') OR (s1 ILIKE 'world%') SETTINGS optimize_or_like_chain = 1, enable_analyzer = 0, max_hyperscan_regexp_length = 5;

-- Partial-rewrite correctness: when one LHS group is rewritten and another is kept as originals
-- (size limits exceeded), the `indexHint` payload must include *all* original branches — using
-- only the rewritten group's originals would prune granules whose rows only match the kept group.
-- The keys `materialize('aaa')` and `materialize('bbb')` are structurally distinct so they form
-- separate groups; with `max_hyperscan_regexp_length = 5`, `s1`'s long prefix patterns are kept,
-- while `s2`'s substring pattern is rewritten to `multiSearchAny`.
EXPLAIN QUERY TREE run_passes=1 SELECT materialize('aaa') AS s1, materialize('bbb') AS s2 WHERE (s1 LIKE 'hellooooo%') OR (s1 ILIKE 'worldddddd%') OR (s2 LIKE '%foo%') SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1, max_hyperscan_regexp_length = 5;

-- Result correctness for the partial-rewrite case. Only one branch matches and that branch is in
-- the kept-original group; with the indexHint bug, the row could be pruned.
SELECT materialize('bbb foo') WHERE materialize('aaa') LIKE 'hellooooo%' OR materialize('bbb foo') LIKE '%foo%' SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1, max_hyperscan_regexp_length = 4;
SELECT materialize('bbb foo') WHERE materialize('aaa') LIKE 'hellooooo%' OR materialize('bbb foo') LIKE '%foo%' SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1, max_hyperscan_regexp_length = 4;
