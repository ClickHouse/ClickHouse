DROP TABLE IF EXISTS x;

CREATE TABLE x ENGINE = Log AS SELECT * FROM numbers(0);

SYSTEM STOP MERGES x;

WITH y AS
    (
        SELECT *
        FROM numbers(10)
    )
INSERT INTO x
SELECT *
FROM y
INTERSECT
SELECT *
FROM numbers(5);

WITH y AS
    (
        SELECT *
        FROM numbers(10)
    )
INSERT INTO x
SELECT *
FROM numbers(5)
INTERSECT
SELECT *
FROM y;

SELECT * FROM x;

DROP TABLE x;

CREATE TABLE x (d date) ENGINE = Log;

WITH y AS
    (
        SELECT
            number,
            date_add(YEAR, number, toDate('2025-01-01')) AS new_date
        FROM numbers(10)
    )
INSERT INTO x
SELECT y.new_date FROM y;

SELECT * FROM x;

DROP TABLE x;


WITH x AS (SELECT 1) INSERT INTO TABLE t0 (c0) WITH y AS (SELECT 1) (SELECT 1); -- { clientError SYNTAX_ERROR }

WITH z AS (SELECT 1) INSERT INTO TABLE x SELECT 1 FROM ((SELECT 1) UNION (WITH y AS (SELECT 1) (SELECT 1) UNION (SELECT 1)));  -- { clientError SYNTAX_ERROR }

WITH x AS (SELECT 1 as c0) INSERT INTO TABLE t0 (c0) SELECT [1,;  -- { clientError SYNTAX_ERROR }