-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104788
-- The function `arrayPrAUC` was introduced in v24.12.1.1614-stable (PR #72073).
-- PR #72950 renamed it to `arrayAUCPR` and registered the all-caps alias `arrayPRAUC`,
-- but forgot the original mixed-case name `arrayPrAUC`. Since function names in
-- ClickHouse are case-sensitive, users upgrading from v24.12.1 got UNKNOWN_FUNCTION.
-- This test guards against re-introducing that regression.

SELECT floor(arrayPrAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]), 10);
SELECT floor(arrayPRAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]), 10);
SELECT floor(arrayAUCPR([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]), 10);

-- All three names must resolve to the same function.
SELECT
    arrayPrAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])
  = arrayAUCPR([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])
  AND
    arrayPRAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])
  = arrayAUCPR([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1]);

-- `system.functions` should list both aliases pointing at the canonical name.
SELECT name, alias_to
FROM system.functions
WHERE name IN ('arrayPrAUC', 'arrayPRAUC')
ORDER BY name;
