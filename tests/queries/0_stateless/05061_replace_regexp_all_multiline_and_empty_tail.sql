-- Tags: no-old-analyzer
-- `optimize_rewrite_regexp_functions` is a query tree pass, and `EXPLAIN QUERY TREE` needs the analyzer.
-- https://github.com/ClickHouse/ClickHouse/issues/116921
-- `optimize_rewrite_regexp_functions` turns `replaceRegexpAll` into `replaceRegexpOne` for an
-- anchored pattern, on the assumption that such a pattern matches at most once. That is wrong when
-- an inline `(?m)` flag makes `$` match at every line end, and when the pattern can also match the
-- empty string at the end of the subject, where a global replace replaces twice.
-- The haystack has to come from a column: a fully constant argument is folded before the rewrite.

SET optimize_rewrite_regexp_functions = 1;

SELECT replaceRegexpAll(h, '(?m)a$', 'Z'), regexp_replace(h, '(?m)a$', 'Z') FROM (SELECT 'a\nb\na' AS h);
SELECT replaceRegexpAll(h, '(?m)a$', 'Z'), regexp_replace(h, '(?m)a$', 'Z') FROM (SELECT 'a\nb\na' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

SELECT replaceRegexpAll(h, '(?im)A$', 'Z') FROM (SELECT 'a\nb\na' AS h);
SELECT replaceRegexpAll(h, '(?im)A$', 'Z') FROM (SELECT 'a\nb\na' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

SELECT replaceRegexpAll(h, 'o*$', 'Z') FROM (SELECT 'foo' AS h);
SELECT replaceRegexpAll(h, 'o*$', 'Z') FROM (SELECT 'foo' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

SELECT replaceRegexpAll(h, 'o?$', 'Z') FROM (SELECT 'foo' AS h);
SELECT replaceRegexpAll(h, 'o?$', 'Z') FROM (SELECT 'foo' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

SELECT replaceRegexpAll(h, '(o|)$', 'Z') FROM (SELECT 'foo' AS h);
SELECT replaceRegexpAll(h, '(o|)$', 'Z') FROM (SELECT 'foo' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

-- A zero-width assertion in front of the trailing `$` does not make the tail have to consume:
-- a global replace matches the trailing `a` and then the empty string at the end of the subject.
SELECT replaceRegexpAll(h, 'a?\\b$', 'Z') FROM (SELECT 'a' AS h);
SELECT replaceRegexpAll(h, 'a?\\b$', 'Z') FROM (SELECT 'a' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

-- A `^`-anchored pattern can only match at offset 0, so a nullable tail is harmless there.
SELECT replaceRegexpAll(h, '^o*', 'Z') FROM (SELECT 'foo' AS h);
SELECT replaceRegexpAll(h, '^o*', 'Z') FROM (SELECT 'foo' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

-- A tail that has to consume a character keeps the rewrite.
SELECT 'still rewritten';
SELECT replaceRegexpAll(h, 'o+$', 'Z') FROM (SELECT 'foo' AS h);
SELECT replaceRegexpAll(h, 'o+$', 'Z') FROM (SELECT 'foo' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('foo'), 'o+$', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('foo'), '(?m)o$', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('foo'), 'o*$', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('a'), 'a?\\b$', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';

-- `\Q...\E` quotes its body, so a `$` inside it is a literal and not an anchor, and an empty body
-- consumes nothing at all. An unterminated `\Q` quotes up to the end of the pattern.
SELECT 'quoted literals';
SELECT replaceRegexpAll(h, 'a?\\Q\\E$', 'Z') FROM (SELECT 'a' AS h);
SELECT replaceRegexpAll(h, 'a?\\Q\\E$', 'Z') FROM (SELECT 'a' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

SELECT replaceRegexpAll(h, 'a\\Q$', 'Z') FROM (SELECT 'a$a$' AS h);
SELECT replaceRegexpAll(h, 'a\\Q$', 'Z') FROM (SELECT 'a$a$' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('a'), 'a?\\Q\\E$', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('a$a$'), 'a\\Q$', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';

-- A quoted literal that has to consume a character in front of a real `$` keeps the rewrite.
SELECT 'still rewritten';
SELECT replaceRegexpAll(h, 'a\\Qxy\\E$', 'Z') FROM (SELECT 'axy' AS h);
SELECT replaceRegexpAll(h, 'a\\Qxy\\E$', 'Z') FROM (SELECT 'axy' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('axy'), 'a\\Qxy\\E$', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';
