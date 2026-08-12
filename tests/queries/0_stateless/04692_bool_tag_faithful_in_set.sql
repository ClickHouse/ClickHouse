-- Bool set elements converted to a textual left-hand type must use the canonical Bool text
-- ('true'/'false'), not the numeric collapse ('1'/'0'). The IN/set path builds the set column-natively
-- via convertColumnToType, which preserves the Bool tag (the previous Field-based path collapsed
-- Bool to UInt64 first, producing '1'/'0'). This mirrors the values() table function fix.
-- Checked on both the analyzer and the legacy execution paths.

SET allow_experimental_analyzer = 1;

SELECT 'scalar' AS shape, 'true' IN (true) AS textual_matches, '1' IN (true) AS numeric_matches;
SELECT 'array' AS shape, 'false' IN [true, false] AS textual_matches, '0' IN [true, false] AS numeric_matches;
SELECT 'multikey' AS shape, ('true', 5) IN ((true, 5)) AS textual_matches, ('1', 5) IN ((true, 5)) AS numeric_matches;
SELECT 'array-of-bool' AS shape, ['true'] IN (CAST([true], 'Array(Bool)')) AS textual_matches;

SET allow_experimental_analyzer = 0;

SELECT 'scalar' AS shape, 'true' IN (true) AS textual_matches, '1' IN (true) AS numeric_matches;
SELECT 'array' AS shape, 'false' IN [true, false] AS textual_matches, '0' IN [true, false] AS numeric_matches;
SELECT 'multikey' AS shape, ('true', 5) IN ((true, 5)) AS textual_matches, ('1', 5) IN ((true, 5)) AS numeric_matches;
SELECT 'array-of-bool' AS shape, ['true'] IN (CAST([true], 'Array(Bool)')) AS textual_matches;
