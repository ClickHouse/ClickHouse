SET enable_analyzer = 1;

CREATE VIEW 03271_parametrized_v AS
SELECT number AS n
FROM numbers({upper_bound:UInt64});

CREATE VIEW 03271_parametrized_v_expl (n UInt64) AS
SELECT number AS n
FROM numbers({upper_bound:UInt64});

-- Should return no columns
SHOW COLUMNS IN 03271_parametrized_v;

-- Separator
SELECT 'SEPARATOR 1';

-- Should return one column 'n' of type 'UInt64'
SHOW COLUMNS IN 03271_parametrized_v_expl;

-- Separator
SELECT 'SEPARATOR 2';

SELECT *
FROM system.columns
WHERE table = '03271_parametrized_v' AND database = currentDatabase();

-- Separator
SELECT 'SEPARATOR 3';

SELECT *
FROM system.columns
WHERE table = '03271_parametrized_v_expl' AND database = currentDatabase();

-- Separator
SELECT 'SEPARATOR 4';

-- Mismatched schema: should return error on query tree building
CREATE VIEW 03271_parametrized_v_expl_mismatch (n UInt64, s String) AS
SELECT number AS n
FROM numbers({upper_bound:UInt64});

SELECT *
FROM 03271_parametrized_v_expl_mismatch(upper_bound = 3); -- { serverError TYPE_MISMATCH }

-- Separator
SELECT 'SEPARATOR 5';

EXPLAIN AST SELECT *
FROM 03271_parametrized_v_expl_mismatch(upper_bound = 3);

-- Separator
SELECT 'SEPARATOR 6';

EXPLAIN QUERY TREE SELECT *
FROM 03271_parametrized_v_expl_mismatch(upper_bound = 3); -- { serverError TYPE_MISMATCH }

SELECT 'SEPARATOR 7';

SELECT *
FROM 03271_parametrized_v_expl(upper_bound = 3);

DROP VIEW 03271_parametrized_v;
DROP VIEW 03271_parametrized_v_expl;
DROP VIEW 03271_parametrized_v_expl_mismatch;
