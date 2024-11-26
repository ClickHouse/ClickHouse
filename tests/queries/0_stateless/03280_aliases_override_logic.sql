SELECT b FROM
(
    SELECT number, number*2
    FROM numbers(2) (a, b)
) as x;

SELECT a FROM
(
    SELECT number, number*2
    FROM numbers(2) (a, b)
) as x;

SELECT a FROM
(
SELECT number, number*2
FROM numbers(10)
) as x (a); -- { serverError BAD_ARGUMENTS }

CREATE VIEW IF NOT EXISTS test_view_03280 (a,b) AS SELECT 1, 2;

SELECT a FROM test_view_03280;

SELECT b FROM test_view_03280;

DROP VIEW IF EXISTS test_view_03280;

CREATE VIEW test_view_1_03280 (a) AS SELECT 1, 2; -- { serverError BAD_ARGUMENTS }
