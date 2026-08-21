-- Tags: shard

-- The formatter must not print a strictness modifier on a `PASTE JOIN`, because the parser rejects
-- one and a formatted query carrying it fails on a remote server with `SYNTAX_ERROR`. It must still
-- print the modifier, and the `PASTE` kind itself, everywhere the parser does accept them.

SET join_default_strictness = 'ALL';
SET any_join_distinct_right_table_keys = 0;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_paste_l;
DROP TABLE IF EXISTS t_paste_r;
CREATE TABLE t_paste_l (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_paste_r (b UInt64) ENGINE = MergeTree ORDER BY b;
INSERT INTO t_paste_l SELECT number FROM numbers(3);
INSERT INTO t_paste_r SELECT number + 10 FROM numbers(3);

-- A: the formatter must not print a strictness modifier on PASTE JOIN, because its own
-- parser rejects one. Every count below is 1 without the fix.
SELECT 'A1', count() FROM (EXPLAIN SYNTAX oneline = 1 SELECT * FROM t_paste_l PASTE JOIN t_paste_r) WHERE explain ILIKE '%ALL PASTE%' OR explain ILIKE '%ANY PASTE%';
SELECT 'A2', count() FROM (EXPLAIN SYNTAX oneline = 1, run_query_tree_passes = 1 SELECT * FROM t_paste_l PASTE JOIN t_paste_r) WHERE explain ILIKE '%ALL PASTE%' OR explain ILIKE '%ANY PASTE%';
SET enable_analyzer = 0;
SELECT 'A3', count() FROM (EXPLAIN SYNTAX oneline = 1 SELECT * FROM t_paste_l PASTE JOIN t_paste_r) WHERE explain ILIKE '%ALL PASTE%' OR explain ILIKE '%ANY PASTE%';
SET enable_analyzer = 1;
SELECT 'A4', count() FROM (EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1, run_passes = 1 SELECT * FROM t_paste_l PASTE JOIN t_paste_r) WHERE explain ILIKE '%ALL PASTE%' OR explain ILIKE '%ANY PASTE%';

-- A7/A8: the other value join_default_strictness accepts. Both normalizers map Unspecified to
-- Any for a Paste kind as well, so the formatter must not print `ANY PASTE JOIN` either.
SET join_default_strictness = 'ANY';
SELECT 'A7', count() FROM (EXPLAIN SYNTAX oneline = 1, run_query_tree_passes = 1 SELECT * FROM t_paste_l PASTE JOIN t_paste_r) WHERE explain ILIKE '%ALL PASTE%' OR explain ILIKE '%ANY PASTE%';
SET enable_analyzer = 0;
SELECT 'A8', count() FROM (EXPLAIN SYNTAX oneline = 1 SELECT * FROM t_paste_l PASTE JOIN t_paste_r) WHERE explain ILIKE '%ALL PASTE%' OR explain ILIKE '%ANY PASTE%';
SET enable_analyzer = 1;
SET join_default_strictness = 'ALL';

-- A9/A10: with any_join_distinct_right_table_keys = 1 both normalizers rewrite Any to RightAny,
-- which the printer maps to the same `ANY ` token, so that enum value needs its own assertions.
SET join_default_strictness = 'ANY';
SET any_join_distinct_right_table_keys = 1;
SELECT 'A9', count() FROM (EXPLAIN SYNTAX oneline = 1, run_query_tree_passes = 1 SELECT * FROM t_paste_l PASTE JOIN t_paste_r) WHERE explain ILIKE '%ALL PASTE%' OR explain ILIKE '%ANY PASTE%';
SET enable_analyzer = 0;
SELECT 'A10', count() FROM (EXPLAIN SYNTAX oneline = 1 SELECT * FROM t_paste_l PASTE JOIN t_paste_r) WHERE explain ILIKE '%ALL PASTE%' OR explain ILIKE '%ANY PASTE%';
SET enable_analyzer = 1;
SET any_join_distinct_right_table_keys = 0;
SET join_default_strictness = 'ALL';

-- A5/A6: the formatted text is what a shipped query carries, so a PASTE JOIN with a remote
-- table in the leftmost slot fails on the remote server with SYNTAX_ERROR without the fix.
SELECT 'A5', * FROM remote('127.0.0.2', currentDatabase(), t_paste_l) AS x PASTE JOIN t_paste_r AS y ORDER BY a;
SET enable_analyzer = 0;
SELECT 'A6', * FROM remote('127.0.0.2', currentDatabase(), t_paste_l) AS x PASTE JOIN t_paste_r AS y ORDER BY a;
SET enable_analyzer = 1;

-- B: the strictness modifier must still be printed everywhere the parser accepts it, and the
-- PASTE kind itself must still be printed. Every count below is 1 with and without the fix.
SELECT 'B1', count() FROM (EXPLAIN SYNTAX oneline = 1 SELECT * FROM t_paste_l AS l ALL INNER JOIN t_paste_l AS r ON l.a = r.a) WHERE explain ILIKE '%ALL INNER JOIN%';
SELECT 'B2', count() FROM (EXPLAIN SYNTAX oneline = 1 SELECT * FROM t_paste_l AS l ANY LEFT JOIN t_paste_l AS r ON l.a = r.a) WHERE explain ILIKE '%ANY LEFT JOIN%';
SELECT 'B3', count() FROM (EXPLAIN SYNTAX oneline = 1 SELECT * FROM t_paste_l AS l SEMI LEFT JOIN t_paste_l AS r ON l.a = r.a) WHERE explain ILIKE '%SEMI LEFT JOIN%';
SELECT 'B4', count() FROM (EXPLAIN SYNTAX oneline = 1 SELECT * FROM t_paste_l AS l ANTI LEFT JOIN t_paste_l AS r ON l.a = r.a) WHERE explain ILIKE '%ANTI LEFT JOIN%';
SELECT 'B5', count() FROM (EXPLAIN SYNTAX oneline = 1 SELECT * FROM t_paste_l AS l ASOF JOIN t_paste_l AS r ON l.a = r.a AND l.a < r.a) WHERE explain ILIKE '%ASOF INNER JOIN%';
SET any_join_distinct_right_table_keys = 1;
SELECT 'B6', count() FROM (EXPLAIN SYNTAX oneline = 1 SELECT * FROM t_paste_l AS l ANY RIGHT JOIN t_paste_l AS r ON l.a = r.a) WHERE explain ILIKE '%ANY RIGHT JOIN%';
SELECT 'B7', count() FROM (EXPLAIN SYNTAX oneline = 1, run_query_tree_passes = 1 SELECT * FROM t_paste_l AS l ANY INNER JOIN t_paste_l AS r ON l.a = r.a) WHERE explain ILIKE '%SEMI LEFT JOIN%';
SET any_join_distinct_right_table_keys = 0;
SELECT 'B8', count() FROM (EXPLAIN SYNTAX oneline = 1 SELECT * FROM t_paste_l PASTE JOIN t_paste_r) WHERE explain ILIKE '%PASTE JOIN%';
-- B9 is intentionally unchanged by this fix: a strictness modifier printed on a NATURAL join is a
-- separate, diagnostics-only carrier, tracked apart from this PR.
SELECT 'B9', count() FROM (EXPLAIN SYNTAX oneline = 1 SELECT * FROM t_paste_l AS l NATURAL LEFT JOIN t_paste_l AS r) WHERE explain ILIKE '%ALL NATURAL%';

-- C: the fix must not resurrect the unparsable NATURAL CROSS JOIN spelling of issue #100220.
SELECT 'C1', count() FROM (EXPLAIN SYNTAX oneline = 1 SELECT * FROM t_paste_l NATURAL JOIN t_paste_r) WHERE explain ILIKE '%NATURAL CROSS JOIN%';

DROP TABLE t_paste_l;
DROP TABLE t_paste_r;
