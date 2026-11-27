-- Parameterized view with a subquery-valued argument of a type that scalar subquery
-- optimization keeps unfolded (`Array`, `Tuple`, `LowCardinality`, ...). Such a value used
-- to stay a `__getScalar` reference instead of a literal, so it was not collected as a view
-- parameter and the query failed with `UNKNOWN_QUERY_PARAMETER`. See issue #68041.
SET enable_analyzer = 1;
-- Pin the scalar subquery optimization on: with it disabled the value would fold to a literal
-- through the pre-existing path, so the regression cases below must run with it enabled.
SET enable_scalar_subquery_optimization = 1;

DROP TABLE IF EXISTS pv_data;
DROP TABLE IF EXISTS pv_ids;
CREATE TABLE pv_data (AccountId Int32, v String) ENGINE = Memory;
INSERT INTO pv_data VALUES (1, 'a') (2, 'b') (3, 'c') (5, 'e');
CREATE TABLE pv_ids (AccountId Int32) ENGINE = Memory;
INSERT INTO pv_ids VALUES (1) (2) (3);

SELECT 'Array(Int32) param';
DROP VIEW IF EXISTS pv_array;
CREATE VIEW pv_array AS SELECT * FROM pv_data WHERE AccountId IN {AccountIds : Array(Int32)};
-- literal and `array` function forms already worked; keep them as guards
SELECT AccountId FROM pv_array(AccountIds = [1, 2, 3]) ORDER BY AccountId;
SELECT AccountId FROM pv_array(AccountIds = array(1, 2, 3)) ORDER BY AccountId;
-- subquery-valued form is the regression from #68041
SELECT AccountId FROM pv_array(AccountIds = (SELECT groupArray(AccountId) FROM pv_ids)) ORDER BY AccountId;
SELECT AccountId FROM pv_array(AccountIds = CAST((SELECT groupArray(AccountId) FROM pv_ids), 'Array(Int32)')) ORDER BY AccountId;
-- empty subquery result: matches nothing
SELECT count() FROM pv_array(AccountIds = (SELECT groupArray(AccountId) FROM pv_ids WHERE AccountId > 100));

SELECT 'Array(String) param';
DROP VIEW IF EXISTS pv_str;
CREATE VIEW pv_str AS SELECT * FROM pv_data WHERE v IN {names : Array(String)};
SELECT AccountId FROM pv_str(names = (SELECT groupArray(v) FROM pv_data WHERE AccountId <= 2)) ORDER BY AccountId;

SELECT 'Tuple param';
DROP TABLE IF EXISTS pv_tuple_data;
CREATE TABLE pv_tuple_data (id Int32, pair Tuple(Int32, Int32)) ENGINE = Memory;
INSERT INTO pv_tuple_data VALUES (1, (10, 20)) (2, (30, 40));
DROP VIEW IF EXISTS pv_tuple;
CREATE VIEW pv_tuple AS SELECT * FROM pv_tuple_data WHERE pair = {p : Tuple(Int32, Int32)};
-- multi-column subquery packages the result as a top-level `Tuple` (a single-column
-- `(SELECT (10, 20))` would be `Nullable(Tuple(...))`, which already folds without this fix)
SELECT id FROM pv_tuple(p = (SELECT toInt32(10), toInt32(20))) ORDER BY id;

SELECT 'LowCardinality(String) param';
DROP TABLE IF EXISTS pv_lc_data;
CREATE TABLE pv_lc_data (id Int32, name LowCardinality(String)) ENGINE = Memory;
INSERT INTO pv_lc_data VALUES (1, 'x') (2, 'y') (3, 'z') (4, 'a\tb');
DROP VIEW IF EXISTS pv_lc;
-- scalar `LowCardinality(String)` param (top-level family `LowCardinality`, not `Array`)
CREATE VIEW pv_lc AS SELECT * FROM pv_lc_data WHERE name = {nm : LowCardinality(String)};
SELECT id FROM pv_lc(nm = (SELECT toLowCardinality('y'))) ORDER BY id;
-- string value with a tab must be text-escaped, otherwise it is parsed as an incomplete value
SELECT id FROM pv_lc(nm = (SELECT toLowCardinality('a\tb'))) ORDER BY id;

SELECT 'Array(LowCardinality(String)) param';
DROP VIEW IF EXISTS pv_lc_arr;
CREATE VIEW pv_lc_arr AS SELECT * FROM pv_lc_data WHERE name IN {names : Array(LowCardinality(String))};
SELECT id FROM pv_lc_arr(names = (SELECT groupArray(name) FROM pv_lc_data WHERE id <= 2)) ORDER BY id;

-- `CAST` wrapping a subquery must also be substituted (during execution and in `EXPLAIN SYNTAX`)
SELECT 'CAST(subquery) param';
SELECT AccountId FROM pv_array(AccountIds = CAST((SELECT groupArray(AccountId) FROM pv_ids), 'Array(Int32)')) ORDER BY AccountId;

-- A subquery whose own predicate references the parameter name must not overwrite the parameter
-- value (the collector must not descend into the value expression).
SELECT 'subquery predicate references param name';
DROP TABLE IF EXISTS pv_self;
CREATE TABLE pv_self (AccountIds Int32) ENGINE = Memory;
INSERT INTO pv_self VALUES (1) (2) (9);
SELECT AccountId FROM pv_array(AccountIds = (SELECT groupArray(AccountIds) FROM pv_self WHERE AccountIds = 9)) ORDER BY AccountId;

-- `EXPLAIN SYNTAX` expands parameterized views through a separate AST path
-- (`analyzeFunctionParamValues`) that also dropped the subquery-valued parameter and threw
-- `UNKNOWN_QUERY_PARAMETER`. These assertions run through that AST path (the queries above run
-- through the analyzer's own path), and check the exact substituted value is present (not just
-- any substitution), so the test also catches a wrong value being substituted. See issue #68041.
SELECT 'EXPLAIN SYNTAX subquery param';
SELECT count() FROM (EXPLAIN SYNTAX SELECT * FROM pv_array(AccountIds = (SELECT groupArray(AccountId) FROM pv_ids))) WHERE explain LIKE '%[1, 2, 3]%';
-- `CAST` wrapping a subquery must also be substituted on the AST path
SELECT count() FROM (EXPLAIN SYNTAX SELECT * FROM pv_array(AccountIds = CAST((SELECT groupArray(AccountId) FROM pv_ids), 'Array(Int32)'))) WHERE explain LIKE '%[1, 2, 3]%';
-- a string value with a tab must be text-escaped by the AST path (otherwise it renders unescaped)
SELECT count() FROM (EXPLAIN SYNTAX SELECT * FROM pv_lc(nm = (SELECT toLowCardinality('a\tb')))) WHERE explain LIKE '%a\\tb%';
-- an inner predicate referencing the parameter name must not overwrite the collected value on the AST path
SELECT count() FROM (EXPLAIN SYNTAX SELECT * FROM pv_array(AccountIds = (SELECT groupArray(AccountIds) FROM pv_self WHERE AccountIds = 9))) WHERE explain LIKE '%[9]%';

-- A direct (non-subquery, non-function) string literal must be text-escaped on the AST path too.
-- A top-level string literal used to be stored as raw bytes, but `ReplaceQueryParameterVisitor`
-- reads it back with `deserializeTextEscaped`, so a value with a tab / newline / backslash was
-- truncated or mis-parsed and the query returned wrong rows or threw `BAD_QUERY_PARAMETER`.
-- Exercise both AST-path entry points: legacy execution (`enable_analyzer = 0`, via
-- `Context::executeTableFunction`) and `EXPLAIN SYNTAX`, for both `String` and `LowCardinality(String)`.
INSERT INTO pv_lc_data VALUES (5, 'c\nd') (6, 'e\\f');
DROP TABLE IF EXISTS pv_str_data;
CREATE TABLE pv_str_data (id Int32, s String) ENGINE = Memory;
INSERT INTO pv_str_data VALUES (10, 'p\tq') (11, 'r\ns') (12, 't\\u');
DROP VIEW IF EXISTS pv_str_scalar;
CREATE VIEW pv_str_scalar AS SELECT * FROM pv_str_data WHERE s = {ss : String};

SELECT 'direct literal param, legacy analyzer';
SET enable_analyzer = 1;

SELECT 'direct literal param, EXPLAIN SYNTAX';
SELECT count() FROM (EXPLAIN SYNTAX SELECT * FROM pv_lc(nm = 'a\tb')) WHERE explain LIKE '%a\\tb%';
SELECT count() FROM (EXPLAIN SYNTAX SELECT * FROM pv_str_scalar(ss = 'p\tq')) WHERE explain LIKE '%p\\tq%';

-- Analyzer header inference (`getSampleBlock`, only-analyze mode, e.g. `CREATE TABLE ... AS
-- SELECT`) must resolve a subquery-valued parameter to its real value, not a default
-- placeholder. When the projected type depends on the parameter value the default (empty
-- array, length 0) made `toFixedString` throw `size must be positive`, so `CREATE TABLE ...
-- AS SELECT` failed while real execution produced `FixedString(3)`. See issue #68041.
SELECT 'analyzer-only header inference, value-dependent type (Array)';
DROP VIEW IF EXISTS pv_fixed;
CREATE VIEW pv_fixed AS SELECT toFixedString('v', length({p : Array(Int32)})) AS s;
DROP TABLE IF EXISTS pv_cts_fixed;
CREATE TABLE pv_cts_fixed ENGINE = Memory AS SELECT s FROM pv_fixed(p = (SELECT groupArray(AccountId) FROM pv_ids));
-- the header-inferred column type must equal the execution type
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'pv_cts_fixed' AND name = 's';
SELECT toTypeName(s) FROM pv_fixed(p = (SELECT groupArray(AccountId) FROM pv_ids)) LIMIT 1;

-- Same only-analyze header path (`CREATE TABLE ... AS SELECT`) across the Tuple /
-- LowCardinality subquery-argument matrix.
SELECT 'analyzer-only header inference across type matrix (Tuple, LowCardinality)';
DROP TABLE IF EXISTS pv_cts_tuple;
CREATE TABLE pv_cts_tuple ENGINE = Memory AS SELECT id FROM pv_tuple(p = (SELECT toInt32(10), toInt32(20)));
SELECT id FROM pv_cts_tuple ORDER BY id;
DROP TABLE IF EXISTS pv_cts_lc;
CREATE TABLE pv_cts_lc ENGINE = Memory AS SELECT id FROM pv_lc(nm = (SELECT toLowCardinality('y')));
SELECT id FROM pv_cts_lc ORDER BY id;

DROP VIEW pv_array;
DROP VIEW pv_str;
DROP VIEW pv_tuple;
DROP VIEW pv_lc;
DROP VIEW pv_lc_arr;
DROP VIEW pv_str_scalar;
DROP VIEW pv_fixed;
DROP TABLE pv_data;
DROP TABLE pv_ids;
DROP TABLE pv_tuple_data;
DROP TABLE pv_lc_data;
DROP TABLE pv_str_data;
DROP TABLE pv_self;
DROP TABLE pv_cts_fixed;
DROP TABLE pv_cts_tuple;
DROP TABLE pv_cts_lc;
