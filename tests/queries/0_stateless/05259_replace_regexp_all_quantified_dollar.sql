-- A quantified trailing `$` is not an anchor: `o$?` matches every `o`, so `replaceRegexpAll` must stay a global replace.
SET enable_analyzer = 1;
SET optimize_rewrite_regexp_functions = 1;

SELECT replaceRegexpAll(materialize('foo'), 'o$?', 'Z'), replaceRegexpAll(materialize('foo'), 'o$*', 'Z'), replaceRegexpAll(materialize('foo'), 'o${0,1}', 'Z');
SELECT replaceRegexpAll(materialize('foo'), 'o$?', 'Z'), replaceRegexpAll(materialize('foo'), 'o$*', 'Z'), replaceRegexpAll(materialize('foo'), 'o${0,1}', 'Z') SETTINGS optimize_rewrite_regexp_functions = 0;

EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(materialize('foo'), 'o$?', 'Z');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT replaceRegexpAll(materialize('foo'), 'o$', 'Z');
