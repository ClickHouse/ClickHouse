SET join_use_nulls = 1;

DROP TABLE IF EXISTS null;
CREATE TABLE null (k UInt64, a String, b Nullable(String)) ENGINE = Log;

INSERT INTO null SELECT
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
)
ANY LEFT JOIN
(
    SELECT
        number AS k,
        toString(number) AS b
    FROM system.numbers
    LIMIT 1, 2
) USING (k)
ORDER BY k ASC;

SELECT * FROM null ORDER BY k, a, b;

DROP TABLE null;
