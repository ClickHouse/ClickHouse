CREATE VIEW 03271_parametrized_v AS
SELECT number AS n
FROM numbers({upper_bound:UInt64});

CREATE VIEW 03271_parametrized_v_expl (n UInt64) AS
SELECT number AS n
FROM numbers({upper_bound:UInt64});

-- Should return no columns
SHOW COLUMNS IN 03271_parametrized_v;

-- Separator
SELECT 1 AS separator;

-- Should return one column 'n' of type 'UInt64'
SHOW COLUMNS IN 03271_parametrized_v_expl;

-- Separator
SELECT 2 AS separator;

SELECT *
FROM system.columns
WHERE table = '03271_parametrized_v';

-- Separator
SELECT 3 AS separator;

SELECT *
FROM system.columns
WHERE table = '03271_parametrized_v_expl';

DROP VIEW 03271_parametrized_v;
DROP VIEW 03271_parametrized_v_expl;
