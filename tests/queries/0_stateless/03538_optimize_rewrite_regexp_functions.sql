-- { echo ON }

SET enable_analyzer = 1;
SET optimize_rewrite_regexp_functions = 1;

-- Rule 1: replaceRegexpAll / regexp_replace -> replaceRegexpOne if pattern without alternatives starts with ^ or ends with unescaped $

-- Starts with ^ (should rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT regexp_replace(identity('abc123'), '^abc', '');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('abc123'), '^abc', '');

-- Ends with unescaped $ (should rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT regexp_replace(identity('abc123'), '123$', '');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('abc123'), '123$', '');

-- Ends with escaped $ (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('abc123$'), '123\$', '');

-- Starts with escaped ^ (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('abc123'), '\^abc', '');

-- Pattern with ^ not at start (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('abc123'), 'a^bc', '');

-- Pattern with $ not at end (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('abc123'), '123$abc', '');

-- Pattern with alternatives (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('abc123'), '^123|456$', '');

-- Rule 2: If a replaceRegexpOne function has a replacement of nothing other than \1 and some subpatterns in the regexp, or \0 and no subpatterns in the regexp, rewrite it with extract.

-- NOTE: \0 is specially treated as NUL instead of capture group reference. Need to use \\0 instead.

-- Only \0, no capture group (should rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpOne(identity('abc123'), '^abc123$', '\\0');

-- Only \1, with one capture group (should rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpOne(identity('abc123'), '^(abc)$', '\1');

-- Only \1, no capture group (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpOne(identity('abc123'), '^abc$', '\1');

-- Pattern not full (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpOne(identity('abc123'), '^abc', '\\0');

-- Pattern not full (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpOne(identity('abc123'), 'abc$', '\\0');

-- Pattern not full (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpOne(identity('abc123'), 'abc', '\\0');

-- Pattern not full (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpOne(identity('abc123'), '^abc\\$', '\\0');

-- Pattern not full (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpOne(identity('abc123'), '^ab|c$', '\\0');

-- \0 with extra characters (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpOne(identity('abc123'), '^abc123$', 'pre\\0post');

-- \1 with two capture groups (should rewrite — only \1 used)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpOne(identity('abc123'), '^(a)(b)$', '\1');

-- \2 used (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpOne(identity('abc123'), '^(a)(b)$', '\2');

-- Mixed content in replacement (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpOne(identity('abc123'), '^(abc)$', 'X\1Y');

-- Escaped backslash in replacement (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpOne(identity('abc123'), '^(abc)$', '\\\\1');


-- Rule 3: If an extract function has a regexp with some subpatterns and the regexp starts with ^.* or ending with an unescaped .*$, remove this prefix and/or suffix.

-- Starts with ^.* (should strip prefix)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT extract(identity('abc123'), '^.*(123)');

-- Ends with unescaped .*$ (should strip suffix)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT extract(identity('abc123'), '(abc).*$');

-- Starts and ends (should strip both)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT extract(identity('abc123'), '^.*(abc).*$');

-- Starts and ends (should NOT rewrite without capture groups)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT extract(identity('abc123'), '^.*$');

-- Escaped dot before * (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT extract(identity('abc123'), '(abc)\.*$');

-- No prefix or suffix (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT extract(identity('abc123'), '(abc)');

-- Starts with .* but not ^.* (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT extract(identity('abc123'), '.*(abc)');

-- Starts with ^.*? (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT extract(identity('abc123abc456'), '^.*?(abc.*)');

-- Ends with .* but not .*$ (should NOT rewrite)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT extract(identity('abc123'), '(abc).*');


-- Cascade tests

-- Rule 1 + Rule 2: replaceRegexpAll to replaceRegexpOne to extract
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('abc'), '^(abc)', '\1');

-- Rule 2 + 3: replaceRegexpOne -> extract -> simplified extract
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpOne(identity('abc'), '^.*(abc).*$','\1');

-- Rule 1 + 2 + 3: replaceRegexpAll -> replaceRegexpOne -> extract -> simplified extract
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('abc'), '^.*(abc).*$','\1');

-- ClickBench Q28
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT REGEXP_REPLACE(identity('some referer'), '^https?://(?:www\.)?([^/]+)/.*$', '\1');
