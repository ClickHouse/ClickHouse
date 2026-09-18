SET enable_analyzer = 1;

SELECT 'basic alignment';
SELECT *
FROM
(
    SELECT 1 AS a, 'one' AS b
    UNION ALL BY NAME
    SELECT 'two' AS b, 3 AS c
);

SELECT 'first-seen order with multiple operands';
SELECT *
FROM
(
    SELECT 1 AS b, 'left' AS a
    UNION ALL BY NAME
    SELECT 'middle' AS c, 2 AS b
    UNION ALL BY NAME
    SELECT 3 AS d, 'right' AS a
);

SELECT 'duplicate rows are preserved';
SELECT a
FROM
(
    SELECT 1 AS a
    UNION ALL BY NAME
    SELECT 1 AS a
);

SELECT 'outer projection: a';
SELECT a
FROM
(
    SELECT 1 AS a, 2 AS b
    UNION ALL BY NAME
    SELECT 3 AS b, 4 AS a
)
ORDER BY a;

SELECT 'outer projection: b';
SELECT b
FROM
(
    SELECT 1 AS a, 2 AS b
    UNION ALL BY NAME
    SELECT 3 AS b, 4 AS a
)
ORDER BY b;

SELECT 'derived table aliases';
SELECT x
FROM
(
    SELECT 1 AS a
    UNION ALL BY NAME
    SELECT 2 AS a
) AS t(x)
ORDER BY x;

SELECT 'name then position';
SELECT *
FROM
(
    SELECT 1 AS a, 'one' AS b
    UNION ALL BY NAME
    SELECT 'two' AS b, 2 AS a
    UNION ALL
    SELECT 3 AS a, 'three' AS b
)
ORDER BY a;

SELECT 'position then name';
SELECT *
FROM
(
    SELECT 1 AS a, 'one' AS b
    UNION ALL
    SELECT 2 AS a, 'two' AS b
    UNION ALL BY NAME
    SELECT 'three' AS b, 3 AS a
);

SELECT 'missing nullable value';
SELECT *
FROM
(
    SELECT 1 AS x, 10 AS y
    UNION ALL BY NAME
    SELECT 1 AS x
)
ORDER BY x, isNull(y);

SELECT 'missing LowCardinality Nullable';
SELECT x, y, toTypeName(x), toTypeName(y)
FROM
(
    SELECT toLowCardinality(CAST('hello', 'Nullable(String)')) AS x
    UNION ALL BY NAME
    SELECT 1 AS y
)
ORDER BY isNull(x), isNull(y);

SELECT 'missing LowCardinality';
SELECT x, y, toTypeName(x), toTypeName(y)
FROM
(
    SELECT toLowCardinality('hello') AS x
    UNION ALL BY NAME
    SELECT 1 AS y
)
ORDER BY isNull(x), isNull(y);

SELECT 'missing Variant';
SELECT x, y, toTypeName(x), toTypeName(y)
FROM
(
    SELECT CAST('hello', 'Variant(String, UInt64)') AS x
    UNION ALL BY NAME
    SELECT 1 AS y
)
ORDER BY isNull(x), isNull(y);

SELECT 'missing Dynamic';
SELECT x, y, toTypeName(x), toTypeName(y)
FROM
(
    SELECT CAST('hello', 'Dynamic') AS x
    UNION ALL BY NAME
    SELECT 1 AS y
)
ORDER BY isNull(x), isNull(y);

SELECT 'AST JSON roundtrip';
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 AS a UNION ALL BY NAME SELECT 2 AS a'));

SELECT 'normalized AST JSON roundtrip';
SELECT formatQueryFromJSON(
    replace(
        replace(
            parseQueryToJSON('SELECT 1 AS a UNION ALL BY NAME SELECT 2 AS a'),
            '"is_normalized":false',
            '"is_normalized":true'),
        '"list_of_column_match_modes":["NAME"]',
        '"list_of_column_match_modes":[]'));

SELECT formatQueryFromJSON(
    replace(
        parseQueryToJSON('SELECT 1 AS a UNION ALL BY NAME SELECT 2 AS a'),
        '"list_of_modes":["UNION_ALL"]',
        '"list_of_modes":[]')); -- { serverError BAD_ARGUMENTS }

SELECT 'duplicate output names';
SELECT 1 AS x, 2 AS x
UNION ALL BY NAME
SELECT 3 AS x; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

SELECT 'missing non-nullable-capable type';
SELECT [1, 2] AS x
UNION ALL BY NAME
SELECT 1 AS y; -- { serverError TYPE_MISMATCH }

SELECT formatQueryFromJSON(
    replace(
        parseQueryToJSON('SELECT 1 AS a UNION ALL BY NAME SELECT 2 AS a'),
        '"list_of_modes":["UNION_ALL"]',
        '"list_of_modes":["UNION_DISTINCT"]')); -- { serverError BAD_ARGUMENTS }

SELECT 'recursive CTE';
WITH RECURSIVE r AS
(
    SELECT 1 AS a
    UNION ALL BY NAME
    SELECT a + 1 AS a FROM r WHERE a < 2
)
SELECT * FROM r; -- { serverError UNSUPPORTED_METHOD }

SELECT 'UNION BY NAME with INTERSECT';
SELECT *
FROM
(
    SELECT 1 AS a
    UNION ALL BY NAME
    SELECT 2 AS a
    INTERSECT ALL
    SELECT 2 AS a
)
ORDER BY a;

SELECT 'EXCEPT with UNION BY NAME';
SELECT *
FROM
(
    SELECT 1 AS a
    EXCEPT ALL
    SELECT 2 AS a
    UNION ALL BY NAME
    SELECT 3 AS a
)
ORDER BY a;

SELECT 'UNION BY NAME with reordered INTERSECT operands';
SELECT *
FROM
(
    SELECT 1 AS a, 'x' AS b
    UNION ALL BY NAME
    SELECT 'y' AS b, 2 AS a
    INTERSECT ALL
    SELECT 'y' AS b, 2 AS a
)
ORDER BY a;
