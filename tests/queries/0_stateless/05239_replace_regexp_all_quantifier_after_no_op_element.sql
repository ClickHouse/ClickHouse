-- Tags: no-old-analyzer
-- `optimize_rewrite_regexp_functions` is a query tree pass, and `EXPLAIN QUERY TREE` needs the analyzer.
-- https://github.com/ClickHouse/ClickHouse/issues/116921
-- re2 pushes no element for an empty `\Q\E` or for an inline flag group such as `(?i)`, so a
-- quantifier right after one binds to the previous element: `a\Q\E?$` and `a(?i)?$` are `a?$`.
-- Such a pattern can match the empty string at the end of the subject, where a global replace
-- replaces once more, so `replaceRegexpAll` must not be rewritten to `replaceRegexpOne`.
-- The haystack has to come from a column: a fully constant argument is folded before the rewrite.

SET optimize_rewrite_regexp_functions = 1;

SELECT replaceRegexpAll(h, 'a\\Q\\E?$', 'Z') FROM (SELECT 'a' AS h);
SELECT replaceRegexpAll(h, 'a\\Q\\E?$', 'Z') FROM (SELECT 'a' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;
SELECT replaceRegexpAll(h, 'a\\Q\\E*$', 'Z') FROM (SELECT 'a' AS h);
SELECT replaceRegexpAll(h, 'a\\Q\\E*$', 'Z') FROM (SELECT 'a' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;
SELECT replaceRegexpAll(h, 'a\\Q\\E{0,1}$', 'Z') FROM (SELECT 'a' AS h);
SELECT replaceRegexpAll(h, 'a\\Q\\E{0,1}$', 'Z') FROM (SELECT 'a' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

SELECT replaceRegexpAll(h, 'a(?i)?$', 'Z') FROM (SELECT 'a' AS h);
SELECT replaceRegexpAll(h, 'a(?i)?$', 'Z') FROM (SELECT 'a' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;
SELECT replaceRegexpAll(h, 'a(?s)*$', 'Z') FROM (SELECT 'a' AS h);
SELECT replaceRegexpAll(h, 'a(?s)*$', 'Z') FROM (SELECT 'a' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;
SELECT replaceRegexpAll(h, 'a(?-i){0,1}$', 'Z') FROM (SELECT 'a' AS h);
SELECT replaceRegexpAll(h, 'a(?-i){0,1}$', 'Z') FROM (SELECT 'a' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('a'), 'a\\Q\\E?$', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('a'), 'a(?i)?$', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';

-- Without a quantifier after them, these elements do not change what the tail has to consume.
SELECT 'still rewritten';
SELECT replaceRegexpAll(h, 'a\\Q\\E$', 'Z') FROM (SELECT 'aa' AS h);
SELECT replaceRegexpAll(h, 'a(?i)$', 'Z') FROM (SELECT 'aa' AS h);
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('aa'), 'a\\Q\\E$', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('aa'), 'a(?i)$', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';
