SELECT b FROM
(
    SELECT number, number*2
    FROM numbers(2)
) AS x (a, b);

SELECT a FROM
(
    SELECT number, number*2
    FROM numbers(2)
) AS x (a, b);

SELECT a FROM
(
    SELECT number, number*2
    FROM numbers(2)
) AS x (a); -- { serverError BAD_ARGUMENTS }

SELECT c FROM
(
    SELECT number, number*2
    FROM numbers(2)
) as x (a, b); -- { serverError UNKNOWN_IDENTIFIER }

DROP VIEW IF EXISTS test_view_03280;

CREATE VIEW test_view_03280 (a,b) AS SELECT 1, 2;

SELECT a FROM test_view_03280;

SELECT b FROM test_view_03280;

DROP VIEW IF EXISTS test_view_03280;

SELECT c FROM test_view_03280;  -- { serverError UNKNOWN_IDENTIFIER }

CREATE VIEW test_view_1_03280 (a) AS SELECT 1, 2; -- { serverError BAD_ARGUMENTS }

EXPLAIN AST CREATE VIEW test_view_1_03280 (a, b] AS SELECT 1, 2; -- { clientError SYNTAX_ERROR }

EXPLAIN AST CREATE VIEW test_view_1_03280 ((a, b)) AS SELECT 1, 2; -- { clientError SYNTAX_ERROR }
