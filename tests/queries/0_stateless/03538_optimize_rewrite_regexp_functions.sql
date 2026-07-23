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

-- Rule 2 (replaceRegexpOne -> extract) was removed because extract returns empty string on non-match,
-- while replaceRegexpOne returns the original string, making them semantically different.

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

-- Rule 1 only: replaceRegexpAll to replaceRegexpOne (Rule 2 removed)
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('abc'), '^(abc)', '\1');

-- ClickBench Q28: Rule 1 only: regexp_replace to replaceRegexpOne
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT REGEXP_REPLACE(identity('some referer'), '^https?://(?:www\.)?([^/]+)/.*$', '\1');
