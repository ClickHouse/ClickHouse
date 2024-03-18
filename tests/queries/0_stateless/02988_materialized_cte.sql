
-- Simple tests

EXPLAIN PIPELINE
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(10))
SELECT * FROM
(
    SELECT * FROM t WHERE (c % 2) = 0
    UNION ALL
    SELECT * FROM t WHERE (c % 2) = 1
);

EXPLAIN PIPELINE
WITH dict AS MATERIALIZED (SELECT number AS key, toString(number) AS value FROM numbers(100)) ENGINE = Join(ANY, LEFT, key)
SELECT *
FROM numbers(100) AS t
ANY LEFT JOIN dict AS d1 ON t.number = dict.key;

EXPLAIN PIPELINE
WITH dict AS MATERIALIZED (SELECT number AS key, toString(number) AS value FROM numbers(100)) ENGINE = Join(ANY, LEFT, key)
SELECT number, joinGet(dict, 'value', number)
FROM numbers(10);

EXPLAIN PIPELINE
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(10)) ENGINE = Set
SELECT * FROM numbers(100) WHERE number IN t;

