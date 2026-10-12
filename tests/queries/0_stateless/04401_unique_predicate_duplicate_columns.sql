SET enable_analyzer = 1;

-- The UNIQUE rewrite filters out rows that contain a NULL in any projected column.
-- The per-column NULL filter must be applied by position, not by name, otherwise a
-- subquery that exposes duplicate or qualified column names would bind the filter to
-- the wrong column and produce a wrong result.

SELECT 'Duplicate qualified names, every row has a NULL (vacuously unique)';
SELECT UNIQUE(SELECT t.x, s.x FROM (SELECT 1 AS x FROM numbers(2)) AS t CROSS JOIN (SELECT CAST(NULL AS Nullable(UInt8)) AS x) AS s);

SELECT 'Duplicate qualified names, no NULL, duplicate rows';
SELECT UNIQUE(SELECT t.x, s.x FROM (SELECT 1 AS x FROM numbers(2)) AS t CROSS JOIN (SELECT 2 AS x) AS s);

SELECT 'Identical column names, all rows distinct';
SELECT UNIQUE(SELECT number, number FROM numbers(3));

SELECT 'Identical column names, duplicate rows';
SELECT UNIQUE(SELECT number % 2, number % 2 FROM numbers(4));

SELECT 'Qualified names with NULLs (join_use_nulls), distinct after filtering';
SET join_use_nulls = 1;
SELECT UNIQUE(SELECT l.id, r.id FROM (SELECT number AS id FROM numbers(3)) AS l LEFT JOIN (SELECT number AS id FROM numbers(3) WHERE number > 0) AS r ON l.id = r.id);
