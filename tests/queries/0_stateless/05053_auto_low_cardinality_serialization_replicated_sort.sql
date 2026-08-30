-- Materializing the automatic (non-native) `LowCardinality` representation must never touch a genuine
-- `LowCardinality(T)` column, including when it is nested inside a `ColumnReplicated` produced by a join.
-- Otherwise the sorted result no longer matches its own data type.

SET max_threads = 4;

SELECT l.key, l.value, r.value
FROM
(
    SELECT toString(number % 3) :: LowCardinality(String) AS key, [toString(number % 3)] :: Array(LowCardinality(String)) AS value
    FROM numbers(6)
) AS l
FULL JOIN
(
    SELECT toString(number % 2) :: String AS key, [toString(number % 2)] :: Array(String) AS value
    FROM numbers(6)
) AS r
ON l.key = r.key
ORDER BY 1, 2, 3;

SELECT '---';

SELECT toTypeName(l.value), dumpColumnStructure(l.value)
FROM
(
    SELECT toString(number % 3) :: LowCardinality(String) AS key, [toString(number % 3)] :: Array(LowCardinality(String)) AS value
    FROM numbers(6)
) AS l
LEFT JOIN
(
    SELECT toString(number % 2) :: String AS key FROM numbers(6)
) AS r
ON l.key = r.key
ORDER BY 1, 2
LIMIT 1;
