-- The pass rewrites `replaceRegexpAll` into `replaceRegexpOne` for a pattern anchored by a trailing
-- `$`, on the assumption that such a pattern matches at most once, at the end of the subject. When
-- the part before the `$` can match the empty string, a global replace matches once more: after the
-- non-empty match at the end it resumes there and finds the empty suffix, so the rewrite drops a
-- replacement: `replaceRegexpAll('foo', 'o*$', 'Z')` is `fZZ`, `replaceRegexpOne` gives `fZ`.
-- The haystack has to come from a column: a fully constant argument is folded before the rewrite.

-- `optimize_rewrite_regexp_functions` is a query tree pass, and `EXPLAIN QUERY TREE` needs the analyzer.
SET enable_analyzer = 1;
SET optimize_rewrite_regexp_functions = 1;

-- Nullable tails, in every quantifier shape: the optimized and the unoptimized result have to agree.
SELECT replaceRegexpAll(h, 'o*$', 'Z'), regexp_replace(h, 'o*$', 'Z') FROM (SELECT 'foo' AS h);
SELECT replaceRegexpAll(h, 'o*$', 'Z'), regexp_replace(h, 'o*$', 'Z') FROM (SELECT 'foo' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

SELECT replaceRegexpAll(h, '.*$', 'Z') FROM (SELECT 'foo' AS h);
SELECT replaceRegexpAll(h, '.*$', 'Z') FROM (SELECT 'foo' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

SELECT replaceRegexpAll(h, '(?:ab)?$', 'Z') FROM (SELECT 'xab' AS h);
SELECT replaceRegexpAll(h, '(?:ab)?$', 'Z') FROM (SELECT 'xab' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

SELECT replaceRegexpAll(h, 'o{0,2}$', 'Z') FROM (SELECT 'foo' AS h);
SELECT replaceRegexpAll(h, 'o{0,2}$', 'Z') FROM (SELECT 'foo' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

SELECT replaceRegexpAll(h, 'o*?$', 'Z') FROM (SELECT 'foo' AS h);
SELECT replaceRegexpAll(h, 'o*?$', 'Z') FROM (SELECT 'foo' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

-- A trailing assertion is looked through: `o*\b$` is as nullable as `o*$`.
SELECT replaceRegexpAll(h, 'o*\\b$', 'Z') FROM (SELECT 'foo' AS h);
SELECT replaceRegexpAll(h, 'o*\\b$', 'Z') FROM (SELECT 'foo' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

-- A capturing group is not analyzed, so it is treated as nullable too.
SELECT replaceRegexpAll(h, '(o|)$', 'Z') FROM (SELECT 'foo' AS h);
SELECT replaceRegexpAll(h, '(o|)$', 'Z') FROM (SELECT 'foo' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

-- A `$` that is quoted or quantified is text, not an anchor: the pattern is not anchored at all.
SELECT replaceRegexpAll(h, 'o\\Q$', 'Z') FROM (SELECT 'fo$o$' AS h);
SELECT replaceRegexpAll(h, 'o\\Q$', 'Z') FROM (SELECT 'fo$o$' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

-- A tail that has to consume is still rewritten: `o+$`, `o{1,}$`, `\d$`, `[o]$` and `o$` match at most once.
SELECT 'still rewritten';
SELECT replaceRegexpAll(h, 'o+$', 'Z') FROM (SELECT 'foo' AS h);
SELECT replaceRegexpAll(h, 'o+$', 'Z') FROM (SELECT 'foo' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;
SELECT replaceRegexpAll(h, 'o{1,}$', 'Z') FROM (SELECT 'foo' AS h);
SELECT replaceRegexpAll(h, 'o{1,}$', 'Z') FROM (SELECT 'foo' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;
SELECT replaceRegexpAll(h, 'o*\\d$', 'Z') FROM (SELECT 'foo1' AS h);
SELECT replaceRegexpAll(h, 'o*\\d$', 'Z') FROM (SELECT 'foo1' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('foo'), 'o*$', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('foo'), '.*$', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('xab'), '(?:ab)?$', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('foo'), 'o*\\b$', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('foo'), 'o\\Q$', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('foo'), 'o+$', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('foo'), 'o{1,}$', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('foo1'), 'o*\\d$', 'Z')) WHERE explain LIKE '%replaceRegexpOne%';
