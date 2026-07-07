SET enable_analyzer = 1;
-- Tuple comparison involving Nullable(Nothing) elements should return Nullable(UInt8), not Nullable(Nothing).
-- This previously caused a type mismatch assertion in MergingSortedAlgorithm when merging streams from GROUPING SETS.

SELECT or(equals(y, (NULL, '')), equals(x, x)), equals(y, materialize(toNullable((NULL, '')))), materialize((456, NULL))
FROM system.one
LEFT ARRAY JOIN [(NULL, ''), (123, 'Hello'), (456, NULL)] AS y, [1, NULL, 3] AS x
WHERE or(equals(y, (NULL, '')), isNotDistinctFrom(x, x))
GROUP BY GROUPING SETS ((y), (1))
ORDER BY ALL DESC NULLS FIRST
FORMAT Null;

-- Simpler cases for tuple comparison with Nothing type
SELECT (1, 2) = (NULL, 2);
SELECT (NULL, 'a') = (1, 'a');
SELECT toTypeName((NULL, 'a') = (1, 'a'));
