-- The alias fallback for an ambiguous column under prefer_column_name_to_alias = 1 applies only to the
-- unqualified name. A compound identifier whose first part names an alias must stay ambiguous.

SET enable_analyzer = 1;
SET prefer_column_name_to_alias = 1;

SELECT CAST(tuple(42), 'Tuple(k UInt8)') AS a, a.k
FROM (SELECT CAST(tuple(1), 'Tuple(k UInt8)') AS a) AS t1, (SELECT 0 AS z) AS t2, (SELECT CAST(tuple(2), 'Tuple(k UInt8)') AS a) AS t3; -- { serverError AMBIGUOUS_IDENTIFIER }

SELECT CAST(tuple(42), 'Tuple(k UInt8)') AS a, a.k
FROM (SELECT CAST(tuple(1), 'Tuple(k UInt8)') AS a) AS t1, (SELECT 0 AS z) AS t2, (SELECT CAST(tuple(2), 'Tuple(k UInt8)') AS a) AS t3
ORDER BY a.k; -- { serverError AMBIGUOUS_IDENTIFIER }

-- the alias itself is still ambiguous-column-overridable by the bare name
SELECT CAST(tuple(42), 'Tuple(k UInt8)') AS a, a
FROM (SELECT CAST(tuple(1), 'Tuple(k UInt8)') AS a) AS t1, (SELECT 0 AS z) AS t2, (SELECT CAST(tuple(2), 'Tuple(k UInt8)') AS a) AS t3;

-- a qualified column name is not an alias lookup either
SELECT t1.a AS x, t3.a AS a, t1.a.k
FROM (SELECT CAST(tuple(1), 'Tuple(k UInt8)') AS a) AS t1, (SELECT 0 AS z) AS t2, (SELECT CAST(tuple(2), 'Tuple(k UInt8)') AS a) AS t3;
