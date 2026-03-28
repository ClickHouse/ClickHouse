CREATE TABLE test_table
(
    `smt` String
)
ENGINE = MergeTree
ORDER BY smt;

WITH
    statement1 AS
    (
        SELECT '' AS name
        FROM test_table
    ),
    statement2 AS
    (
        SELECT '' AS name
        FROM test_table
    )
SELECT
    statement1.*,
    statement2.*; -- { serverError UNKNOWN_IDENTIFIER }

WITH
  t as (SELECT sum(number) as x FROM numbers(10))
SELECT t.*; -- { serverError UNKNOWN_IDENTIFIER }

WITH a AS (SELECT 1) SELECT a.* FROM (SELECT 1 FROM a); -- { serverError UNKNOWN_IDENTIFIER }

WITH
  t as (SELECT sum(number) as x FROM numbers(10)),
  t1 as (SELECT t.* FROM numbers(1))
SELECT * FROM t1; -- { serverError UNKNOWN_IDENTIFIER }
