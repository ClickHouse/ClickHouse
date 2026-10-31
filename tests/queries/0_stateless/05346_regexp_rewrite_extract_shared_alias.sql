-- Random settings limits: optimize_rewrite_regexp_functions=(1, None)

SET enable_identifier_resolve_cache = 1;

DROP TABLE IF EXISTS t_regexp_shared;
CREATE TABLE t_regexp_shared (s String, ns Nullable(String), n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_regexp_shared SELECT toString(number % 3), if(number = 5, NULL, toString(number % 3)), number FROM numbers(12);

SELECT extract(s, '(1).*$.*$') AS r FROM t_regexp_shared GROUP BY r ORDER BY r;
SELECT extract(s, '(1).*$.*$') AS r FROM t_regexp_shared WHERE r = '1' GROUP BY r;
SELECT extract(s, '(1).*$.*$.*$') AS r FROM t_regexp_shared GROUP BY r HAVING r != 'x' ORDER BY r;
SELECT extract(s, '(1).*$.*$') AS x, x AS y FROM t_regexp_shared GROUP BY y ORDER BY y;
SELECT extract(ns, '(1).*$.*$') AS r FROM t_regexp_shared GROUP BY r ORDER BY r;
SELECT extract(s, '(1)\\.*$.*$') AS r FROM t_regexp_shared GROUP BY r ORDER BY r;

EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT extract(identity('abc123'), '(abc).*$.*$');
EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1 SELECT extract(identity('abc123'), '(abc)\\.*$.*$');

DROP TABLE t_regexp_shared;
