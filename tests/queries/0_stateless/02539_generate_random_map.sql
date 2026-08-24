-- Check that max length works

SELECT max(length(mapKeys(a)))
FROM
(
    SELECT a
    FROM generateRandom('a Map(String, String)', 20, 5, 1)
    LIMIT 1000
);

SELECT max(length(mapKeys(a)))
FROM
(
    SELECT a
    FROM generateRandom('a Map(String, String)', 20, 5, 20)
    LIMIT 1000
);
