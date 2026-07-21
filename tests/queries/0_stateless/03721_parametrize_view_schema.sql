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

-- Schema exposure is decided by the setting value at CREATE/ATTACH/reload time, not at
-- query time (a deliberate backward-compatibility choice).
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
-- Created while the setting was off: turning it on later must not retroactively expose it.
SHOW COLUMNS IN 03271_parametrized_v_off;
CREATE OR REPLACE VIEW 03271_parametrized_v_reload (n UInt64) AS
SELECT number AS n
FROM numbers({upper_bound:UInt64});
SET use_declared_schema_for_parameterized_views = 0;
DETACH TABLE 03271_parametrized_v_reload;
ATTACH TABLE 03271_parametrized_v_reload;
SET use_declared_schema_for_parameterized_views = 1;
-- Reloaded under a default-off load context: the declared schema is dropped and re-enabling
-- the setting afterwards does not restore it.
SHOW COLUMNS IN 03271_parametrized_v_reload;

-- { echoOff }

DROP VIEW 03271_parametrized_v;
DROP VIEW 03271_parametrized_v_expl;
DROP VIEW 03271_parametrized_v_expl_mismatch;
DROP VIEW 03271_parametrized_v_toggle;
DROP VIEW 03271_parametrized_v_toggle_mismatch;
DROP VIEW 03271_parametrized_v_off;
DROP VIEW 03271_parametrized_v_reload;
