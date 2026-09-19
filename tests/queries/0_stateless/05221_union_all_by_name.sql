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

SELECT 'projection pruning: a';
SELECT a
FROM
(
    SELECT 1 AS a, 2 AS b
    UNION ALL BY NAME
    SELECT 3 AS b, 4 AS a
);

SELECT 'projection pruning: b';
SELECT b
FROM
(
    SELECT 1 AS a, 2 AS b
    UNION ALL BY NAME
    SELECT 3 AS b, 4 AS a
);

SELECT 'derived table aliases';
SELECT x
FROM
(
    SELECT 1 AS a
    UNION ALL BY NAME
    SELECT 2 AS a
) AS t(x);

SELECT 'name then position';
SELECT *
FROM
(
    SELECT 1 AS a, 'one' AS b
    UNION ALL BY NAME
    SELECT 'two' AS b, 2 AS a
    UNION ALL
    SELECT 3 AS a, 'three' AS b
);

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
);

SELECT 'AST JSON roundtrip';
SELECT formatQueryFromJSON(parseQueryToJSON('SELECT 1 AS a UNION ALL BY NAME SELECT 2 AS a'));

SELECT 'duplicate output names';
SELECT 1 AS x, 2 AS x
UNION ALL BY NAME
SELECT 3 AS x; -- { serverError BAD_ARGUMENTS }

SET enable_analyzer = 0;
SELECT 1 AS x
UNION ALL BY NAME
SELECT 2 AS x; -- { serverError UNSUPPORTED_METHOD }
SET enable_analyzer = 1;

SELECT 'missing non-nullable-capable type';
SELECT [1, 2] AS x
UNION ALL BY NAME
SELECT 1 AS y; -- { serverError TYPE_MISMATCH }

SELECT 'recursive CTE';
WITH RECURSIVE r AS
(
    SELECT 1 AS a
    UNION ALL BY NAME
    SELECT a + 1 AS a FROM r WHERE a < 2
)
SELECT * FROM r; -- { serverError UNSUPPORTED_METHOD }