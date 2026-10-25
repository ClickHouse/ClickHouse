-- Tags: no-old-analyzer
--       no-old-analyzer: the old analyzer never resolves a subcolumn of an ALIAS parent, so every
--       arm would raise UNKNOWN_IDENTIFIER.

-- Each arm prints the value read through the Merge table next to the same value read straight from
-- the child. They must agree, and the truth must differ from the type default, or a broken read
-- returning the default would still pass.

DROP TABLE IF EXISTS t_arr;
DROP TABLE IF EXISTS m_arr;
CREATE TABLE t_arr (n UInt64, tiny UInt8, arr Array(UInt8) ALIAS [1, 2, 3, 4, 5]) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_arr (n, tiny) VALUES (77, 0);
CREATE TABLE m_arr (arr Array(UInt8), tiny UInt8, n UInt64) ENGINE = Merge(currentDatabase(), '^t_arr$');

SELECT 'array size0', (SELECT arr.size0 FROM m_arr) AS through_merge, (SELECT arr.size0 FROM t_arr) AS direct SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'array size0 subcolumns on', (SELECT arr.size0 FROM m_arr) AS through_merge, (SELECT arr.size0 FROM t_arr) AS direct SETTINGS optimize_functions_to_subcolumns = 1;

-- The subcolumn and its parent in one row: both must describe the same value.
SELECT 'array size0 with parent', arr.size0, length(arr) FROM m_arr SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'array size0 with other column', arr.size0, tiny FROM m_arr SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'array size0 via merge()', (SELECT arr.size0 FROM merge(currentDatabase(), '^t_arr$')) AS through_merge, (SELECT arr.size0 FROM t_arr) AS direct SETTINGS optimize_functions_to_subcolumns = 0;

-- Here the alias expression reads a physical column, so the child must be asked for `dep`.
-- `dep + 2` keeps the size0 truth (9), `dep` (7) and `first` (11) distinct: with a plain
-- `range(dep)` the truth would equal `dep`, and a read landing on `dep` would look correct.
DROP TABLE IF EXISTS t_dep;
DROP TABLE IF EXISTS m_dep;
CREATE TABLE t_dep (first UInt64, tiny UInt8, dep UInt64, arr Array(UInt64) ALIAS range(dep + 2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dep (first, tiny, dep) VALUES (11, 0, 7);
CREATE TABLE m_dep (arr Array(UInt64), tiny UInt8, dep UInt64, first UInt64) ENGINE = Merge(currentDatabase(), '^t_dep$');

SELECT 'dep alias size0', (SELECT arr.size0 FROM m_dep) AS through_merge, (SELECT arr.size0 FROM t_dep) AS direct SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'dep alias size0 with parent', arr.size0, length(arr) FROM m_dep SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'dep alias size0 with dep', arr.size0, dep, first FROM m_dep SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS t_tup;
DROP TABLE IF EXISTS m_tup;
CREATE TABLE t_tup (n UInt64, tup Tuple(a UInt8, b UInt8) ALIAS tuple(1, 2)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_tup (n) VALUES (1);
CREATE TABLE m_tup (tup Tuple(a UInt8, b UInt8), n UInt64) ENGINE = Merge(currentDatabase(), '^t_tup$');
SELECT 'tuple element', (SELECT tup.a FROM m_tup) AS through_merge, (SELECT tup.a FROM t_tup) AS direct SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS t_str;
DROP TABLE IF EXISTS m_str;
CREATE TABLE t_str (n UInt64, s String ALIAS 'hello') ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_str (n) VALUES (1);
CREATE TABLE m_str (s String, n UInt64) ENGINE = Merge(currentDatabase(), '^t_str$');
SELECT 'string size', (SELECT s.size FROM m_str) AS through_merge, (SELECT s.size FROM t_str) AS direct SETTINGS optimize_functions_to_subcolumns = 0;

-- The alias value is NULL, so the truth is 1 and cannot be confused with the UInt8 default.
DROP TABLE IF EXISTS t_null;
DROP TABLE IF EXISTS m_null;
CREATE TABLE t_null (n UInt64, v Nullable(UInt64) ALIAS CAST(NULL, 'Nullable(UInt64)')) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_null (n) VALUES (1);
CREATE TABLE m_null (v Nullable(UInt64), n UInt64) ENGINE = Merge(currentDatabase(), '^t_null$');
SELECT 'nullable null', (SELECT v.null FROM m_null) AS through_merge, (SELECT v.null FROM t_null) AS direct SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS t_nest;
DROP TABLE IF EXISTS m_nest;
CREATE TABLE t_nest (n UInt64, tup Tuple(a Tuple(b UInt8), c UInt8) ALIAS tuple(tuple(4), 9)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nest (n) VALUES (1);
CREATE TABLE m_nest (tup Tuple(a Tuple(b UInt8), c UInt8), n UInt64) ENGINE = Merge(currentDatabase(), '^t_nest$');
SELECT 'nested element', (SELECT tup.a.b FROM m_nest) AS through_merge, (SELECT tup.a.b FROM t_nest) AS direct SETTINGS optimize_functions_to_subcolumns = 0;

-- Map has no readable subcolumn on the Merge table either, so this stays a loud error.
DROP TABLE IF EXISTS t_map;
DROP TABLE IF EXISTS m_map;
CREATE TABLE t_map (n UInt64, mp Map(String, UInt8) ALIAS map('k', 1)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_map (n) VALUES (1);
CREATE TABLE m_map (mp Map(String, UInt8), n UInt64) ENGINE = Merge(currentDatabase(), '^t_map$');
SELECT mp.size0 FROM m_map SETTINGS optimize_functions_to_subcolumns = 0; -- { serverError NO_SUCH_COLUMN_IN_TABLE }

-- A row policy on the child must still bound which rows the subcolumn is computed over.
DROP TABLE IF EXISTS t_pol;
DROP TABLE IF EXISTS m_pol;
CREATE TABLE t_pol (n UInt64, arr Array(UInt8) ALIAS [1, 2, 3, 4, 5]) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_pol (n) VALUES (1), (2), (3);
CREATE TABLE m_pol (arr Array(UInt8), n UInt64) ENGINE = Merge(currentDatabase(), '^t_pol$');
DROP ROW POLICY IF EXISTS p_04738 ON t_pol;
CREATE ROW POLICY p_04738 ON t_pol USING n > 1 TO ALL;
SELECT 'row policy', (SELECT sum(arr.size0) FROM m_pol) AS through_merge, (SELECT sum(arr.size0) FROM t_pol) AS direct SETTINGS optimize_functions_to_subcolumns = 0;
DROP ROW POLICY p_04738 ON t_pol;

-- Parents whose subcolumns are registered were already correct and must stay untouched.
DROP TABLE IF EXISTS t_mat;
DROP TABLE IF EXISTS m_mat;
CREATE TABLE t_mat (n UInt64, arr Array(UInt8) MATERIALIZED [1, 2, 3]) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_mat (n) VALUES (1);
CREATE TABLE m_mat (arr Array(UInt8), n UInt64) ENGINE = Merge(currentDatabase(), '^t_mat$');
SELECT 'materialized parent', (SELECT arr.size0 FROM m_mat) AS through_merge, (SELECT arr.size0 FROM t_mat) AS direct SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS t_def;
DROP TABLE IF EXISTS m_def;
CREATE TABLE t_def (n UInt64, arr Array(UInt8) DEFAULT [1, 2, 3, 4]) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_def (n) VALUES (1);
CREATE TABLE m_def (arr Array(UInt8), n UInt64) ENGINE = Merge(currentDatabase(), '^t_def$');
SELECT 'default parent', (SELECT arr.size0 FROM m_def) AS through_merge, (SELECT arr.size0 FROM t_def) AS direct SETTINGS optimize_functions_to_subcolumns = 0;

-- A child that really lacks the column still gets the type default, which is what makes Merge over
-- differing schemas work. t_miss_b has no arr; only t_miss_a contributes a value.
DROP TABLE IF EXISTS t_miss_a;
DROP TABLE IF EXISTS t_miss_b;
DROP TABLE IF EXISTS m_miss;
CREATE TABLE t_miss_a (n UInt64, arr Array(UInt8), al UInt8 ALIAS 9) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_miss_a (n, arr) VALUES (1, [1, 2, 3]);
CREATE TABLE t_miss_b (n UInt64, al UInt8 ALIAS 9) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_miss_b (n) VALUES (2);
CREATE TABLE m_miss (n UInt64, arr Array(UInt8)) ENGINE = Merge(currentDatabase(), '^t_miss_');
SELECT 'missing column', n, arr.size0 FROM m_miss ORDER BY n SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE IF EXISTS t_arr;
DROP TABLE IF EXISTS m_arr;
DROP TABLE IF EXISTS t_dep;
DROP TABLE IF EXISTS m_dep;
DROP TABLE IF EXISTS t_tup;
DROP TABLE IF EXISTS m_tup;
DROP TABLE IF EXISTS t_str;
DROP TABLE IF EXISTS m_str;
DROP TABLE IF EXISTS t_null;
DROP TABLE IF EXISTS m_null;
DROP TABLE IF EXISTS t_nest;
DROP TABLE IF EXISTS m_nest;
DROP TABLE IF EXISTS t_map;
DROP TABLE IF EXISTS m_map;
DROP TABLE IF EXISTS t_pol;
DROP TABLE IF EXISTS m_pol;
DROP TABLE IF EXISTS t_mat;
DROP TABLE IF EXISTS m_mat;
DROP TABLE IF EXISTS t_def;
DROP TABLE IF EXISTS m_def;
DROP TABLE IF EXISTS t_miss_a;
DROP TABLE IF EXISTS t_miss_b;
DROP TABLE IF EXISTS m_miss;
