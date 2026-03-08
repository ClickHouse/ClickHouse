-- Tags: no-fasttest
-- no-fasttest: Requires vectorscan
SET allow_hyperscan = 1, max_hyperscan_regexp_length = 0, max_hyperscan_regexp_total_length = 0;
SET optimize_rewrite_like_perfect_affix = 0; -- prevent input/output intereference from another LIKE rewrite pass

EXPLAIN SYNTAX SELECT materialize('Привет, World') AS s WHERE (s LIKE 'hell%') OR (s ILIKE '%привет%') OR (s ILIKE 'world%') SETTINGS optimize_or_like_chain = 0;
EXPLAIN QUERY TREE run_passes=1 SELECT materialize('Привет, World') AS s WHERE (s LIKE 'hell%') OR (s ILIKE '%привет%') OR (s ILIKE 'world%') SETTINGS optimize_or_like_chain = 0, enable_analyzer = 1;
EXPLAIN SYNTAX SELECT materialize('Привет, World') AS s WHERE (s LIKE 'hell%') OR (s ILIKE '%привет%') OR (s ILIKE 'world%') SETTINGS optimize_or_like_chain = 1;
EXPLAIN QUERY TREE run_passes=1 SELECT materialize('Привет, World') AS s WHERE (s LIKE 'hell%') OR (s ILIKE '%привет%') OR (s ILIKE 'world%') SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;

EXPLAIN SYNTAX SELECT materialize('Привет, World') AS s1, materialize('Привет, World') AS s2 WHERE (s1 LIKE 'hell%') OR (s2 ILIKE '%привет%') OR (s1 ILIKE 'world%') SETTINGS optimize_or_like_chain = 1;
EXPLAIN SYNTAX SELECT materialize('Привет, World') AS s1, materialize('Привет, World') AS s2 WHERE (s1 LIKE 'hell%') OR (s2 ILIKE '%привет%') OR (s1 ILIKE 'world%') SETTINGS optimize_or_like_chain = 1, allow_hyperscan = 0;
EXPLAIN SYNTAX SELECT materialize('Привет, World') AS s1, materialize('Привет, World') AS s2 WHERE (s1 LIKE 'hell%') OR (s2 ILIKE '%привет%') OR (s1 ILIKE 'world%') SETTINGS optimize_or_like_chain = 1, max_hyperscan_regexp_length = 10;
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

-- Test match() function combined with LIKE (should use multiMatchAny)
EXPLAIN SYNTAX SELECT materialize('Hello World') AS s WHERE (s LIKE 'hello%') OR match(s, 'wor.*') SETTINGS optimize_or_like_chain = 1;
EXPLAIN QUERY TREE run_passes=1 SELECT materialize('Hello World') AS s WHERE (s LIKE 'hello%') OR match(s, 'wor.*') SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;

-- Verify match() combined with LIKE returns correct results
SELECT materialize('Hello World') AS s WHERE (s LIKE 'hello%') OR match(s, 'wor.*') SETTINGS optimize_or_like_chain = 1;
SELECT materialize('Hello World') AS s WHERE (s LIKE 'hello%') OR match(s, 'wor.*') SETTINGS optimize_or_like_chain = 0;

-- Test multiple match() functions
EXPLAIN SYNTAX SELECT materialize('test string') AS s WHERE match(s, '^test') OR match(s, 'ing$') SETTINGS optimize_or_like_chain = 1;
SELECT materialize('test string') AS s WHERE match(s, '^test') OR match(s, 'ing$') SETTINGS optimize_or_like_chain = 1;

-- Test pure substring patterns (should use multiSearchAny instead of multiMatchAny)
EXPLAIN SYNTAX SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s LIKE '%World%') SETTINGS optimize_or_like_chain = 1;
EXPLAIN QUERY TREE run_passes=1 SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s LIKE '%World%') SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;

-- Verify multiSearchAny returns correct results
SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s LIKE '%World%') SETTINGS optimize_or_like_chain = 1;
SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s LIKE '%World%') SETTINGS optimize_or_like_chain = 0;

-- Test that no match still returns empty
SELECT materialize('test') AS s WHERE (s LIKE '%Hello%') OR (s LIKE '%World%') SETTINGS optimize_or_like_chain = 1;

-- Test case-insensitive substring patterns (should use multiSearchAnyCaseInsensitive)
EXPLAIN SYNTAX SELECT materialize('Hello World') AS s WHERE (s ILIKE '%hello%') OR (s ILIKE '%world%') SETTINGS optimize_or_like_chain = 1;
EXPLAIN QUERY TREE run_passes=1 SELECT materialize('Hello World') AS s WHERE (s ILIKE '%hello%') OR (s ILIKE '%world%') SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;

-- Verify case-insensitive multiSearchAny returns correct results
SELECT materialize('Hello World') AS s WHERE (s ILIKE '%hello%') OR (s ILIKE '%world%') SETTINGS optimize_or_like_chain = 1;
SELECT materialize('Hello World') AS s WHERE (s ILIKE '%hello%') OR (s ILIKE '%world%') SETTINGS optimize_or_like_chain = 0;

-- Test mixed case sensitivity (should fall back to multiMatchAny)
EXPLAIN SYNTAX SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s ILIKE '%world%') SETTINGS optimize_or_like_chain = 1;
EXPLAIN QUERY TREE run_passes=1 SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s ILIKE '%world%') SETTINGS optimize_or_like_chain = 1, enable_analyzer = 1;

-- Verify mixed case still returns correct results
SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s ILIKE '%world%') SETTINGS optimize_or_like_chain = 1;
SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s ILIKE '%world%') SETTINGS optimize_or_like_chain = 0;

-- Test substring patterns with non-substring patterns (should use multiMatchAny)
EXPLAIN SYNTAX SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s LIKE 'World%') SETTINGS optimize_or_like_chain = 1;

-- Verify mixed pattern types still returns correct results
SELECT materialize('Hello World') AS s WHERE (s LIKE '%Hello%') OR (s LIKE 'World%') SETTINGS optimize_or_like_chain = 1;
SELECT materialize('World Hello') AS s WHERE (s LIKE '%Hello%') OR (s LIKE 'World%') SETTINGS optimize_or_like_chain = 1;

-- Test match() with case-insensitive regexp
EXPLAIN SYNTAX SELECT materialize('Hello World') AS s WHERE match(s, '(?i)hello') OR match(s, '(?i)world') SETTINGS optimize_or_like_chain = 1;
SELECT materialize('Hello World') AS s WHERE match(s, '(?i)hello') OR match(s, '(?i)world') SETTINGS optimize_or_like_chain = 1;
