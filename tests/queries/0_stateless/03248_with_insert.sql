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