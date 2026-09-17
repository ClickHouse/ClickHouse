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