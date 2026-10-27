-- https://github.com/ClickHouse/ClickHouse/issues/116922
-- `optimize_rewrite_regexp_functions` stripped a leading greedy `^.*` from an `extract` pattern.
-- The greedy prefix consumes as much as it can and backtracks, so the capture binds at the last
-- offset where the rest of the pattern matches, while the stripped pattern binds at the first one.
-- `^.*(\d)` is the idiomatic "last digit" pattern.
-- The haystack has to come from a column: a fully constant argument is folded before the rewrite.

SET enable_analyzer = 1;
SET optimize_rewrite_regexp_functions = 1;

SELECT extract(h, '^.*(\\d)'), extract(h, '^.*(\\d+)'), extract(h, '^.*([a-z])') FROM (SELECT 'a1b2c3' AS h);
SELECT extract(h, '^.*(\\d)'), extract(h, '^.*(\\d+)'), extract(h, '^.*([a-z])') FROM (SELECT 'a1b2c3' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

SELECT extract(h, '^.*(\\d).*$') FROM (SELECT 'a1b2c3' AS h);
SELECT extract(h, '^.*(\\d).*$') FROM (SELECT 'a1b2c3' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;

-- Stripping a trailing `.*$` is sound: leftmost-first matching fixes the match start first.
SELECT 'suffix still stripped';
SELECT extract(h, '(\\d).*$') FROM (SELECT 'a1b2c3' AS h);
SELECT extract(h, '(\\d).*$') FROM (SELECT 'a1b2c3' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT extract(identity('abc123'), '(abc).*$')) WHERE explain LIKE '%\'(abc)\'%';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT extract(identity('abc123'), '^.*(123)')) WHERE explain LIKE '%\'^.*(123)\'%';

-- https://github.com/ClickHouse/ClickHouse/issues/117236
-- An inline `(?-s)` turns off `dot_nl`, and then a trailing `.*$` pins the match to the last line.
SELECT 'suffix kept when the pattern disables dot-all';
SELECT extract(h, '(?-s)(\\d+).*$') FROM (SELECT '11\n22\n33' AS h);
SELECT extract(h, '(?-s)(\\d+).*$') FROM (SELECT '11\n22\n33' AS h) SETTINGS optimize_rewrite_regexp_functions = 0;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT extract(identity('11\n22\n33'), '(?-s)(\\d+).*$')) WHERE explain LIKE '%.*$%';
