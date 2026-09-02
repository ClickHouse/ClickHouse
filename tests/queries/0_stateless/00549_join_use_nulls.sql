SET join_use_nulls = 1;

DROP TABLE IF EXISTS null_00549;
CREATE TABLE null_00549 (k UInt64, a String, b Nullable(String)) ENGINE = Log;

INSERT INTO null_00549 SELECT
    k,
    a,
    b
FROM
(
    SELECT
        number AS k,
        toString(number) AS a
    FROM system.numbers
    LIMIT 2
) js1
ANY LEFT JOIN
(
    SELECT
        number AS k,
        toString(number) AS b
    FROM system.numbers
    LIMIT 1, 2
) js2 USING (k)
ORDER BY k ASC;

SELECT * FROM null_00549 ORDER BY k, a, b;

DROP TABLE null_00549;
