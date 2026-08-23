SET enable_analyzer = 1;

-- { echoOn }
SET use_declared_schema_for_parameterized_views = 0;

CREATE VIEW 03271_parametrized_v AS
SELECT number AS n
FROM numbers({upper_bound:UInt64});

CREATE VIEW 03271_parametrized_v_expl (n UInt64) AS
SELECT number AS n
FROM numbers({upper_bound:UInt64});

-- Should return no columns
SHOW COLUMNS IN 03271_parametrized_v;

-- Should return no columns
SHOW COLUMNS IN 03271_parametrized_v_expl;

-- Should return no columns
SELECT *
FROM system.columns
WHERE table = '03271_parametrized_v' AND database = currentDatabase();

-- Should return no columns
SELECT *
FROM system.columns
WHERE table = '03271_parametrized_v_expl' AND database = currentDatabase();

-- Mismatched schema: should return no error
CREATE VIEW 03271_parametrized_v_expl_mismatch (n UInt64, s String) AS
SELECT number AS n
FROM numbers({upper_bound:UInt64});

SELECT *
FROM 03271_parametrized_v_expl_mismatch(upper_bound = 3);

EXPLAIN AST SELECT *
FROM 03271_parametrized_v_expl_mismatch(upper_bound = 3);

EXPLAIN QUERY TREE SELECT *
FROM 03271_parametrized_v_expl_mismatch(upper_bound = 3);

SELECT *
FROM 03271_parametrized_v_expl(upper_bound = 3);

SET use_declared_schema_for_parameterized_views = 1;

CREATE OR REPLACE VIEW 03271_parametrized_v AS
SELECT number AS n
FROM numbers({upper_bound:UInt64});

CREATE OR REPLACE VIEW 03271_parametrized_v_expl (n UInt64) AS
SELECT number AS n
FROM numbers({upper_bound:UInt64});

-- Should return no columns
SHOW COLUMNS IN 03271_parametrized_v;

-- Should return one column 'n' of type 'UInt64'
SHOW COLUMNS IN 03271_parametrized_v_expl;

-- Should return no columns
SELECT *
FROM system.columns
WHERE table = '03271_parametrized_v' AND database = currentDatabase();

-- Should return one column 'n' of type 'UInt64'
SELECT *
FROM system.columns
WHERE table = '03271_parametrized_v_expl' AND database = currentDatabase();

-- Mismatched schema: should throw errors now
CREATE OR REPLACE VIEW 03271_parametrized_v_expl_mismatch (n UInt64, s String) AS
SELECT number AS n
FROM numbers({upper_bound:UInt64});

SELECT *
FROM 03271_parametrized_v_expl_mismatch(upper_bound = 3); -- { serverError TYPE_MISMATCH }

-- DESCRIBE must enforce the same declared schema as SELECT.
DESCRIBE TABLE 03271_parametrized_v_expl_mismatch(upper_bound = 3); -- { serverError TYPE_MISMATCH }

EXPLAIN AST SELECT *
FROM 03271_parametrized_v_expl_mismatch(upper_bound = 3);

EXPLAIN QUERY TREE SELECT *
FROM 03271_parametrized_v_expl_mismatch(upper_bound = 3); -- { serverError TYPE_MISMATCH }

SELECT *
FROM 03271_parametrized_v_expl(upper_bound = 3);

-- Lifecycle: the declared schema must survive reload from stored metadata (DETACH/ATTACH).
DETACH TABLE 03271_parametrized_v_expl;
ATTACH TABLE 03271_parametrized_v_expl;
DETACH TABLE 03271_parametrized_v_expl_mismatch;
ATTACH TABLE 03271_parametrized_v_expl_mismatch;

-- Should still return one column 'n' of type 'UInt64' after reload
SHOW COLUMNS IN 03271_parametrized_v_expl;

-- Should still return one column 'n' of type 'UInt64' after reload
SELECT *
FROM system.columns
WHERE table = '03271_parametrized_v_expl' AND database = currentDatabase();

-- Matching schema still succeeds after reload
SELECT *
FROM 03271_parametrized_v_expl(upper_bound = 3);

-- Mismatched schema still throws after reload
SELECT *
FROM 03271_parametrized_v_expl_mismatch(upper_bound = 3); -- { serverError TYPE_MISMATCH }

SET enable_analyzer = 0;
SET use_declared_schema_for_parameterized_views = 1;

-- Legacy path: mismatched schema should also throw TYPE_MISMATCH
SELECT *
FROM 03271_parametrized_v_expl_mismatch(upper_bound = 3); -- { serverError TYPE_MISMATCH }

-- Legacy path: matching schema should succeed
SELECT *
FROM 03271_parametrized_v_expl(upper_bound = 3);

-- Schema exposure is decided by the setting value at CREATE time, not at query time
-- (a deliberate backward-compatibility choice), and the decision is persisted in the stored
-- definition so that reloading it never consults the setting again.
SET enable_analyzer = 1;
SET use_declared_schema_for_parameterized_views = 1;
CREATE OR REPLACE VIEW 03271_parametrized_v_toggle (n UInt64) AS
SELECT number AS n
FROM numbers({upper_bound:UInt64});
SET use_declared_schema_for_parameterized_views = 0;
-- Created while the setting was on: turning it off later must not hide the declared schema.
SHOW COLUMNS IN 03271_parametrized_v_toggle;
SELECT *
FROM system.columns
WHERE table = '03271_parametrized_v_toggle' AND database = currentDatabase();
-- Validation is latched together with exposure: a view that advertises a declared schema also
-- enforces it, regardless of the current setting value. A matching schema still executes...
SELECT *
FROM 03271_parametrized_v_toggle(upper_bound = 3);
SET use_declared_schema_for_parameterized_views = 1;
CREATE OR REPLACE VIEW 03271_parametrized_v_toggle_mismatch (n UInt64, s String) AS
SELECT number AS n
FROM numbers({upper_bound:UInt64});
SET use_declared_schema_for_parameterized_views = 0;
-- ...and a latched mismatching schema is still both exposed and enforced with the setting off,
-- so SHOW COLUMNS and execution stay consistent instead of advertising one schema and running another.
SHOW COLUMNS IN 03271_parametrized_v_toggle_mismatch;
SELECT *
FROM 03271_parametrized_v_toggle_mismatch(upper_bound = 3); -- { serverError TYPE_MISMATCH }
CREATE OR REPLACE VIEW 03271_parametrized_v_off (n UInt64) AS
SELECT number AS n
FROM numbers({upper_bound:UInt64});
SET use_declared_schema_for_parameterized_views = 1;
-- Created while the setting was off: the declared column list is not part of the stored
-- definition, so turning the setting on later must not retroactively expose it...
SHOW COLUMNS IN 03271_parametrized_v_off;
-- ...not even across a reload that happens while the setting is on.
DETACH TABLE 03271_parametrized_v_off;
ATTACH TABLE 03271_parametrized_v_off;
SHOW COLUMNS IN 03271_parametrized_v_off;

-- The declared schema is latched into the stored definition at CREATE time, so reloading it
-- must not depend on the node-local setting value at load time: otherwise one replicated
-- definition would expose (and enforce) different schemas on different replicas.
CREATE OR REPLACE VIEW 03271_parametrized_v_reload (n UInt64) AS
SELECT number AS n
FROM numbers({upper_bound:UInt64});
CREATE OR REPLACE VIEW 03271_parametrized_v_reload_mismatch (n UInt64, s String) AS
SELECT number AS n
FROM numbers({upper_bound:UInt64});
SET use_declared_schema_for_parameterized_views = 0;
DETACH TABLE 03271_parametrized_v_reload;
ATTACH TABLE 03271_parametrized_v_reload;
DETACH TABLE 03271_parametrized_v_reload_mismatch;
ATTACH TABLE 03271_parametrized_v_reload_mismatch;
-- Reloaded under a default-off load context: the declared schema is still exposed...
SHOW COLUMNS IN 03271_parametrized_v_reload;
-- ...still enforced for a matching view...
SELECT *
FROM 03271_parametrized_v_reload(upper_bound = 3);
-- ...and still enforced for a mismatching one, on every node and after every restart.
SELECT *
FROM 03271_parametrized_v_reload_mismatch(upper_bound = 3); -- { serverError TYPE_MISMATCH }

-- A declared schema is exposed through every schema-introspection path, so `DESCRIBE TABLE` of a
-- parameterized view whose stored definition declares a column list returns that schema instead of
-- rejecting the object...
DESCRIBE TABLE 03271_parametrized_v_reload;
-- ...while a parameterized view without a declared schema still cannot be described without
-- parameters, because its columns are only known after substitution.
DESCRIBE TABLE 03271_parametrized_v; -- { serverError UNSUPPORTED_METHOD }

-- { echoOff }

DROP VIEW 03271_parametrized_v;
DROP VIEW 03271_parametrized_v_expl;
DROP VIEW 03271_parametrized_v_expl_mismatch;
DROP VIEW 03271_parametrized_v_toggle;
DROP VIEW 03271_parametrized_v_toggle_mismatch;
DROP VIEW 03271_parametrized_v_off;
DROP VIEW 03271_parametrized_v_reload;
DROP VIEW 03271_parametrized_v_reload_mismatch;
