-- https://github.com/ClickHouse/ClickHouse/issues/92982
-- NOT_FOUND_COLUMN_IN_BLOCK exception for valid queries with constant columns
-- and DISTINCT, ORDER BY, LIMIT BY combinations.
--
-- Root cause: ActionsChainStep::finalizeInputAndOutputColumns kept COLUMN nodes
-- (constants) in DAG outputs instead of INPUT nodes, causing removeUnusedActions
-- to garbage-collect the INPUT and break column propagation across chain steps.

-- Original reproducer
SELECT DISTINCT 1, '1' ORDER BY 1 LIMIT 1 BY 2 SETTINGS enable_analyzer=1;
SELECT DISTINCT 1, '1' ORDER BY 1 LIMIT 1 BY 2 SETTINGS enable_analyzer=0;

-- Multiple constant types
SELECT DISTINCT 1, '1', 2.5 ORDER BY 1 LIMIT 1 BY 2 SETTINGS enable_analyzer=1;

-- Without DISTINCT
SELECT 1, '1' ORDER BY 1 LIMIT 1 BY 2 SETTINGS enable_analyzer=1;

-- Without ORDER BY
SELECT DISTINCT 1, '1' LIMIT 1 BY 2 SETTINGS enable_analyzer=1;

-- Mixed table column + constant
SELECT DISTINCT number % 2, 'const' FROM numbers(5) ORDER BY 1 LIMIT 1 BY 2 SETTINGS enable_analyzer=1;

-- Multiple constants in LIMIT BY
SELECT DISTINCT 1, '1', 2 ORDER BY 1 LIMIT 1 BY '1', 2 SETTINGS enable_analyzer=1;

-- Constant not in LIMIT BY must still survive through the pipeline
SELECT number % 3 AS n, 'tag' AS t FROM numbers(9) ORDER BY n LIMIT 1 BY n SETTINGS enable_analyzer=1;

-- LIMIT BY with GROUP BY and constant in SELECT
SELECT number % 2 AS g, count() AS c, 'label' AS lbl FROM numbers(10) GROUP BY g ORDER BY g LIMIT 1 BY lbl SETTINGS enable_analyzer=1;

-- LIMIT BY with WHERE and constant
SELECT number AS n, 'filtered' AS f FROM numbers(10) WHERE number > 5 ORDER BY n LIMIT 1 BY f SETTINGS enable_analyzer=1;

-- LIMIT 2 BY constant (all rows in one group)
SELECT number AS n, 'grp' AS g FROM numbers(5) ORDER BY n LIMIT 2 BY g SETTINGS enable_analyzer=1;

-- Constant-folded expression in LIMIT BY
SELECT number AS n, 1 + 1 AS two FROM numbers(5) ORDER BY n LIMIT 1 BY two SETTINGS enable_analyzer=1;

-- Regression for the redefinition guard in `finalizeInputAndOutputColumns`:
-- when a subquery redefines a column alias to a different constant value, the
-- outer (redefined) value must be preserved, not silently replaced by the
-- upstream INPUT. Without the guard, the COLUMN-to-INPUT substitution would
-- restore the inner value.
SELECT 'new' AS my_field FROM (SELECT 'old' AS my_field) ORDER BY 1 LIMIT 1 BY my_field SETTINGS enable_analyzer=1;
SELECT DISTINCT 'new' AS my_field FROM (SELECT 'old' AS my_field, number FROM numbers(3)) ORDER BY 1 LIMIT 1 BY my_field SETTINGS enable_analyzer=1;

-- Same constant value but different types: `toUInt8(1)` upstream vs `toUInt16(1)`
-- in the outer scope. The fields compare equal but the types differ, so the
-- type-equality part of the guard must prevent the substitution to preserve
-- the redefined type.
SELECT toUInt16(1) AS x, number FROM (SELECT toUInt8(1) AS x, number FROM numbers(3)) ORDER BY number LIMIT 1 BY x SETTINGS enable_analyzer=1;
SELECT toTypeName(x) FROM (SELECT toUInt16(1) AS x FROM (SELECT toUInt8(1) AS x, number FROM numbers(3)) ORDER BY number LIMIT 1 BY x) SETTINGS enable_analyzer=1;
