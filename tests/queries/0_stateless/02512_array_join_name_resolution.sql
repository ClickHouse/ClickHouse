DROP TABLE IF EXISTS x;
CREATE TABLE x ( `arr.key` Array(String), `arr.value` Array(String), `n` String ) ENGINE = Memory;
INSERT INTO x VALUES (['Hello', 'World'], ['abc', 'def'], 'test');

SELECT
    key,
    any(toString(n))
FROM
(
    SELECT
        arr.key AS key,
        n
    FROM x
    ARRAY JOIN arr
)
GROUP BY key
ORDER BY key;

DROP TABLE x;
