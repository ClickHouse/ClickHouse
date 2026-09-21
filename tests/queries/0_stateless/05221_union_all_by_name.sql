SET enable_analyzer = 1;

SELECT 'basic alignment';
SELECT *
FROM
(
    SELECT 1 AS a, 'one' AS b
    UNION ALL BY NAME
    SELECT 'two' AS b, 3 AS c
)
ORDER BY isNull(a), a;

SELECT 'pipe UNION ALL BY NAME';
SELECT *
FROM
(
    SELECT 1 AS a, 'one' AS b
    |> UNION ALL BY NAME (SELECT 'two' AS b, 3 AS c)
)
ORDER BY isNull(a), a;

SELECT 'first-seen order with multiple operands';
SELECT *
FROM
(
    SELECT 1 AS b, 'left' AS a
    UNION ALL BY NAME
    SELECT 'middle' AS c, 2 AS b
    UNION ALL BY NAME
    SELECT 3 AS d, 'right' AS a
)
ORDER BY isNull(b), b;

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

SELECT 'missing selected column pruning';
SELECT a
FROM
(
    SELECT 1 AS a
    UNION ALL BY NAME
    SELECT throwIf(1) AS b
)
ORDER BY isNull(a), a;

SELECT 'derived table aliases';
SELECT x
FROM
(
    SELECT 1 AS a
    UNION ALL BY NAME
    SELECT 2 AS a
) AS t(x)
ORDER BY x;

SELECT 'nested BY NAME under a single-child wrapper';
SELECT (
    SELECT sum(a)
    FROM
    (
        SELECT 1 AS a
        UNION ALL BY NAME
        SELECT 2 AS a
    )
);

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

SELECT 'mixed positional sibling pruning';
SELECT a
FROM
(
    (
        SELECT 1 AS a, toUInt8(10) AS b
        UNION ALL BY NAME
        SELECT toUInt8(20) AS b, 2 AS a
    )
    UNION ALL
    SELECT toUInt8(number + 3) AS a, throwIf(number = 0) AS unused
    FROM numbers(1)
)
ORDER BY a;

SELECT 'mixed union result alias pruning';
SELECT x
FROM
(
    (SELECT 1 AS a, 2 AS b UNION ALL BY NAME SELECT 3 AS b, 4 AS a)
    UNION ALL
    SELECT 5 AS c, 6 AS d
) AS t(x, y)
ORDER BY x;

SELECT 'position then name';
SELECT *
FROM
(
    SELECT 1 AS a, 'one' AS b
    UNION ALL
    SELECT 2 AS a, 'two' AS b
    UNION ALL BY NAME
    SELECT 'three' AS b, 3 AS a
)
ORDER BY a;

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
            replace(
                replace(
                    replace(
                        parseQueryToJSON('SELECT 1 AS a UNION ALL BY NAME SELECT 2 AS a'),
                        '"union_mode":"UNION_DEFAULT"',
                        '"union_mode":"UNION_ALL"'),
                    '"column_match_mode":"POSITION"',
                    '"column_match_mode":"NAME"'),
                '"is_normalized":false',
                '"is_normalized":true'),
            '"list_of_modes":["UNION_ALL"]',
            '"list_of_modes":[]'),
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

SELECT 'recursive CTE with nested BY NAME';
WITH RECURSIVE r AS
(
    (SELECT 1 AS a UNION ALL BY NAME SELECT 2 AS a)
    UNION ALL
    SELECT a + 1 AS a FROM r WHERE a < 2
)
SELECT min(a), max(a) FROM r;

SELECT 'recursive CTE with nested BY NAME in recursive member';
WITH RECURSIVE r AS
(
    SELECT toUInt64(1) AS a, toUInt64(10) AS b
    UNION ALL
    (
        SELECT a + 1 AS a, b + 10 AS b FROM r WHERE a < 2
        UNION ALL BY NAME
        SELECT b + 20 AS b, a + 2 AS a FROM r WHERE a < 2
    )
)
SELECT a, b FROM r ORDER BY a, b;

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

SELECT 'view result aliases';
DROP TABLE IF EXISTS union_by_name_aliases;
CREATE VIEW union_by_name_aliases(x, y) AS
SELECT 1 AS a, 2 AS b UNION ALL BY NAME SELECT 3 AS b, 4 AS a;
SELECT * FROM union_by_name_aliases ORDER BY x;
SELECT x FROM union_by_name_aliases ORDER BY x;
DROP TABLE union_by_name_aliases;

SELECT 'derived table result alias pruning';
SELECT y
FROM
(
    SELECT throwIf(1) AS a, 2 AS b
    UNION ALL BY NAME
    SELECT 3 AS b, 4 AS a
) AS union_by_name_aliases(x, y)
ORDER BY y;

SELECT 'view result aliases matching operand names';
CREATE VIEW union_by_name_aliases(b, a) AS
SELECT 1 AS a, 2 AS b UNION ALL BY NAME SELECT 3 AS b, 4 AS a;
SELECT * FROM union_by_name_aliases ORDER BY b;
DROP TABLE union_by_name_aliases;

SELECT 'view result aliases with missing columns';
CREATE VIEW union_by_name_aliases(x, y, z) AS
SELECT 1 AS a, 2 AS b UNION ALL BY NAME SELECT 3 AS b, 4 AS c;
SELECT * FROM union_by_name_aliases ORDER BY isNull(x), x;
DROP TABLE union_by_name_aliases;

SELECT 'view result aliases with mixed unions';
CREATE VIEW union_by_name_aliases(x, y) AS
(SELECT 1 AS a, 2 AS b UNION ALL BY NAME SELECT 3 AS b, 4 AS a)
UNION ALL SELECT 5 AS c, 6 AS d;
SELECT * FROM union_by_name_aliases ORDER BY x;
DROP TABLE union_by_name_aliases;

SELECT 'view result alias count';
CREATE VIEW union_by_name_aliases(x) AS SELECT 1 AS a, 2 AS b UNION ALL BY NAME SELECT 3 AS b, 4 AS a; -- { serverError BAD_ARGUMENTS }
CREATE VIEW union_by_name_aliases(x, y, z) AS SELECT 1 AS a, 2 AS b UNION ALL BY NAME SELECT 3 AS b, 4 AS a; -- { serverError BAD_ARGUMENTS }

SELECT 'positional view aliases are unchanged';
CREATE VIEW union_by_name_aliases(x, y) AS
SELECT 1 AS a, 2 AS b UNION ALL SELECT 3 AS b, 4 AS a;
SELECT * FROM union_by_name_aliases ORDER BY x;
DROP TABLE union_by_name_aliases;

SELECT 'CREATE TABLE result columns';
CREATE TABLE union_by_name_aliases(x UInt8, y UInt8) ENGINE = Memory AS
SELECT 1 AS a, 2 AS b UNION ALL BY NAME SELECT 3 AS b, 4 AS a;
SELECT * FROM union_by_name_aliases ORDER BY x;
DROP TABLE union_by_name_aliases;
