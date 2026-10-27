-- The pass rewrites `replaceRegexpAll` into `replaceRegexpOne` for a pattern anchored by `^` or by a
-- trailing `$`, on the assumption that such a pattern matches at most once. An inline `(?m)` makes
-- both anchors match at every line boundary, so the pattern matches once per line and the rewrite
-- would drop every match but the first.
-- The haystack has to come from a column: a fully constant argument is folded before the rewrite.

-- `optimize_rewrite_regexp_functions` is a query tree pass, and `EXPLAIN QUERY TREE` needs the analyzer.
SET enable_analyzer = 1;
SET optimize_rewrite_regexp_functions = 1;

SELECT replaceRegexpAll(h, '(?m)\\d+$', 'x'), regexp_replace(h, '(?m)\\d+$', 'x') FROM (SELECT '11\n22\n33' AS h);
SELECT replaceRegexpAll(h, '(?m)\\d+$', 'x'), regexp_replace(h, '(?m)\\d+$', 'x') FROM (SELECT '11\n22\n33' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

SELECT replaceRegexpAll(h, '(?m)^\\d+', 'x') FROM (SELECT '11\n22\n33' AS h);
SELECT replaceRegexpAll(h, '(?m)^\\d+', 'x') FROM (SELECT '11\n22\n33' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

SELECT replaceRegexpAll(h, '(?im:A)$', 'x') FROM (SELECT 'a\nb\na' AS h);
SELECT replaceRegexpAll(h, '(?im:A)$', 'x') FROM (SELECT 'a\nb\na' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

-- The flag has to be turned on: `(?-m)` and `(?i)` leave the anchors alone.
SELECT 'still rewritten';
SELECT replaceRegexpAll(h, '(?-m)\\d+$', 'x') FROM (SELECT '11\n22\n33' AS h);
SELECT replaceRegexpAll(h, '(?-m)\\d+$', 'x') FROM (SELECT '11\n22\n33' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('11\n22\n33'), '(?m)\\d+$', 'x')) WHERE explain LIKE '%replaceRegexpOne%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('11\n22\n33'), '(?m)^\\d+', 'x')) WHERE explain LIKE '%replaceRegexpOne%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('11\n22\n33'), '(?-m)\\d+$', 'x')) WHERE explain LIKE '%replaceRegexpOne%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('11\n22\n33'), '\\d+$', 'x')) WHERE explain LIKE '%replaceRegexpOne%';
