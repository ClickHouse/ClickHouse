-- A column added by `ALTER TABLE ... ADD COLUMN ... DEFAULT ...` is absent from the blocks inserted before.
-- It and the inputs of its expression stay in the main branch of lazy materialization, and the results
-- are the same as without it. A missing member of a `Nested` gets the sizes of its siblings in both branches.

SET query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000;
SET max_threads = 1;

DROP TABLE IF EXISTS t_memory_lazy_default;
CREATE TABLE t_memory_lazy_default (x UInt64, payload String, other String) ENGINE = Memory;
INSERT INTO t_memory_lazy_default SELECT number, toString(number), 'o' FROM numbers(10);
ALTER TABLE t_memory_lazy_default ADD COLUMN y UInt64 DEFAULT x + 1, ADD COLUMN z String DEFAULT payload || '!';
INSERT INTO t_memory_lazy_default SELECT number + 100, toString(number + 100), 'o', number, 'z' FROM numbers(10);

SELECT '-- explain';
SELECT replaceRegexpOne(explain, '^[ │├└─]*', '') FROM (EXPLAIN actions = 1 SELECT x, y, z, payload, other FROM t_memory_lazy_default ORDER BY x LIMIT 5) WHERE explain LIKE '%Lazily read columns%';

SELECT '-- results';
SELECT x, y, z, payload, other FROM t_memory_lazy_default ORDER BY y DESC, x LIMIT 5;
SELECT x, y, z, payload, other FROM t_memory_lazy_default ORDER BY y DESC, x LIMIT 5 SETTINGS query_plan_optimize_lazy_materialization = 0;
SELECT x, y, z, payload, other FROM t_memory_lazy_default ORDER BY x LIMIT 5;
SELECT x, y, z, payload, other FROM t_memory_lazy_default ORDER BY x LIMIT 5 SETTINGS query_plan_optimize_lazy_materialization = 0;

DROP TABLE t_memory_lazy_default;

DROP TABLE IF EXISTS t_memory_lazy_nested;
CREATE TABLE t_memory_lazy_nested (k UInt64, n Nested(a UInt64)) ENGINE = Memory;
INSERT INTO t_memory_lazy_nested SELECT number, range(number % 4) FROM numbers(10);
ALTER TABLE t_memory_lazy_nested ADD COLUMN `n.b` Array(String);

SELECT '-- nested';
SELECT replaceRegexpOne(explain, '^[ │├└─]*', '') FROM (EXPLAIN actions = 1 SELECT k, n.b FROM t_memory_lazy_nested ORDER BY k DESC LIMIT 4) WHERE explain LIKE '%Lazily read columns%';
SELECT k, n.b, length(n.b) FROM t_memory_lazy_nested ORDER BY k DESC LIMIT 4;
SELECT k, n.b, length(n.b) FROM t_memory_lazy_nested ORDER BY k DESC LIMIT 4 SETTINGS query_plan_optimize_lazy_materialization = 0;

DROP TABLE t_memory_lazy_nested;
