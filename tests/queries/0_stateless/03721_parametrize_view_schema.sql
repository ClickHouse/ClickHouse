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

SET enable_analyzer = 0;
SET use_declared_schema_for_parameterized_views = 1;

-- Legacy path: mismatched schema should also throw TYPE_MISMATCH
SELECT *
FROM 03271_parametrized_v_expl_mismatch(upper_bound = 3); -- { serverError TYPE_MISMATCH }

-- Legacy path: matching schema should succeed
SELECT *
FROM 03271_parametrized_v_expl(upper_bound = 3);

-- { echoOff }

DROP VIEW 03271_parametrized_v;
DROP VIEW 03271_parametrized_v_expl;
DROP VIEW 03271_parametrized_v_expl_mismatch;
