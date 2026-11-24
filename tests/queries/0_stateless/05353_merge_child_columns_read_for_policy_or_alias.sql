-- A Merge column missing from a child is filled with its default, also when the child reads
-- extra columns of its own for its row policy or its ALIAS columns.
DROP ROW POLICY IF EXISTS p_k ON c;
DROP ROW POLICY IF EXISTS p_k ON cn;
DROP ROW POLICY IF EXISTS p_na ON cn;
DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS m2;
DROP TABLE IF EXISTS mn;
DROP TABLE IF EXISTS ma;
DROP TABLE IF EXISTS c;
DROP TABLE IF EXISTS c2;
DROP TABLE IF EXISTS cn;
DROP TABLE IF EXISTS ca;

CREATE TABLE c (k UInt8, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO c VALUES (7, 'hello');
CREATE TABLE m (k UInt8, s String, x Nullable(UInt8), y String) ENGINE = Merge(currentDatabase(), '^c$');
CREATE ROW POLICY p_k ON c USING k > 0 AS PERMISSIVE TO ALL;

SELECT 'policy, x', x FROM m;
SELECT 'policy, y', y FROM m;
SELECT 'policy, x and s', x, s FROM m;
SELECT 'policy, x IS NULL', count() FROM m WHERE x IS NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'policy, no prewhere', x FROM m SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;

CREATE TABLE c2 (k UInt8, s String, x Nullable(UInt8)) ENGINE = MergeTree ORDER BY k;
INSERT INTO c2 VALUES (9, 'world', 5);
CREATE TABLE m2 (k UInt8, s String, x Nullable(UInt8), y String) ENGINE = Merge(currentDatabase(), '^c2?$');
SELECT 'policy on one of two children', x, _table FROM m2 ORDER BY _table;

CREATE TABLE cn (k UInt8, `n.a` Array(UInt8)) ENGINE = MergeTree ORDER BY k;
INSERT INTO cn VALUES (1, [1, 2]);
CREATE TABLE mn (k UInt8, `n.a` Array(UInt8), `n.b` Array(Nullable(UInt8)), x Nullable(UInt8)) ENGINE = Merge(currentDatabase(), '^cn$');
CREATE ROW POLICY p_k ON cn USING k > 0 AS PERMISSIVE TO ALL;
SELECT 'nested, policy on k', n.b, n.a FROM mn;
DROP ROW POLICY p_k ON cn;
CREATE ROW POLICY p_na ON cn USING length(n.a) > 0 AS PERMISSIVE TO ALL;
SELECT 'nested, policy on n.a, n.b', n.b FROM mn;
SELECT 'nested, policy on n.a, x', x FROM mn;

CREATE TABLE ca (k UInt8, y UInt16 ALIAS k * 2) ENGINE = MergeTree ORDER BY k;
INSERT INTO ca VALUES (7);
CREATE TABLE ma (k UInt8, x Nullable(UInt8), y UInt16) ENGINE = Merge(currentDatabase(), '^ca$');
SELECT 'alias, no policy', x, y FROM ma;

DROP ROW POLICY p_k ON c;
DROP ROW POLICY p_na ON cn;
DROP TABLE m;
DROP TABLE m2;
DROP TABLE mn;
DROP TABLE ma;
DROP TABLE c;
DROP TABLE c2;
DROP TABLE cn;
DROP TABLE ca;
