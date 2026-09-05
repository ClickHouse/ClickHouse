-- `optimize_rewrite_regexp_functions` decided from raw bytes alone that a trailing `.*$` (for
-- `extract`) or `$` (for `replaceRegexpAll`) is regexp syntax. re2 quotes everything after an
-- unterminated `\Q` as literal text up to the end of the pattern, so in such a pattern those bytes
-- are ordinary characters: they neither anchor the match nor can be removed.
-- The haystack has to come from a column: a fully constant argument is folded before the rewrite.

SET enable_analyzer = 1;
SET optimize_rewrite_regexp_functions = 1;

-- `(\d)\Q.*$` matches a digit followed by the literal text `.*$`, so `1x` does not match at all.
SELECT 'extract, quoted tail';
SELECT extract(h, '(\\d)\\Q.*$') FROM (SELECT materialize('1x') AS h);
SELECT extract(h, '(\\d)\\Q.*$') FROM (SELECT materialize('1x') AS h) SETTINGS optimize_rewrite_regexp_functions = 0;
SELECT extract(h, '(\\d)\\Q.*$') FROM (SELECT materialize('1.*$x') AS h);
SELECT extract(h, '(\\d)\\Q.*$') FROM (SELECT materialize('1.*$x') AS h) SETTINGS optimize_rewrite_regexp_functions = 0;
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT extract(identity('1x'), '(\\d)\\Q.*$');

-- A terminated `\Q...\E` leaves the tail as regexp syntax, so it is still stripped.
SELECT 'extract, closed quote';
SELECT extract(h, '(\\d)\\Qx\\E.*$') FROM (SELECT materialize('1xy') AS h);
SELECT extract(h, '(\\d)\\Qx\\E.*$') FROM (SELECT materialize('1xy') AS h) SETTINGS optimize_rewrite_regexp_functions = 0;
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT extract(identity('1xy'), '(\\d)\\Qx\\E.*$');

-- `\Q$` matches a literal `$`, so the pattern is not anchored and every occurrence is replaced.
SELECT 'replaceRegexpAll, quoted dollar';
SELECT replaceRegexpAll(h, '\\Q$', '!') FROM (SELECT materialize('x$x$') AS h);
SELECT replaceRegexpAll(h, '\\Q$', '!') FROM (SELECT materialize('x$x$') AS h) SETTINGS optimize_rewrite_regexp_functions = 0;
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('x$x$'), '\\Q$', '!');

-- A real anchor outside the quoted section still allows the rewrite to `replaceRegexpOne`.
SELECT 'replaceRegexpAll, real anchor';
SELECT replaceRegexpAll(h, '\\Qx\\E$', '!') FROM (SELECT materialize('x$x') AS h);
SELECT replaceRegexpAll(h, '\\Qx\\E$', '!') FROM (SELECT materialize('x$x') AS h) SETTINGS optimize_rewrite_regexp_functions = 0;
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(identity('x$x'), '\\Qx\\E$', '!');
