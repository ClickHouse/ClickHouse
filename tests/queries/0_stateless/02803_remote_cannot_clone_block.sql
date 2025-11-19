DROP TABLE IF EXISTS numbers_10_00223;

CREATE TABLE numbers_10_00223
ENGINE = Log AS
SELECT *
FROM system.numbers
LIMIT 10000;

SET enable_analyzer = 0;

SELECT *
FROM
(
    SELECT 1
    FROM remote('127.0.0.{2,3}', currentDatabase(), numbers_10_00223)
        WITH TOTALS
)
WHERE 1
GROUP BY 1;

DROP TABLE numbers_10_00223;
