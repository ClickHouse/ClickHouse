-- Tags: no-backward-compatibility-check

-- https://github.com/ClickHouse/ClickHouse/issues/37561

SELECT NULL
FROM
    (SELECT NULL) AS s1,
    (SELECT count(2), count(1)) AS s2
;

SELECT NULL
FROM
    (SELECT NULL) AS s1,
    (SELECT count(2.), 9223372036854775806, count('-1'), NULL) AS s2,
    (SELECT count('-2147483648')) AS any_query, (SELECT NULL) AS check_single_query
;
