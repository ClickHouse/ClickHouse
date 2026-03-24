SET enable_analyzer = 1;
SET enable_materialized_cte = 1;
SET optimize_group_by_function_keys = 1;

CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=Memory;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

SELECT
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                explain,
                '_temporary_and_external_tables._tmp_\\w+\\-\\w+\\-\\w+\\-\\w+\\-\\w+',
                '_temporary_and_external_tables._tmp_UNIQ_ID'
            ),
            '_materialized_cte_\\w+\\_\\w+',
            '_materialized_cte_UNIQ_ID'
        ),
        '(\\bid|source_id): \\d+',
        '\\1: N'
    )
FROM
(
    EXPLAIN QUERY TREE
    WITH
    a AS MATERIALIZED (SELECT * FROM users),
    (x -> x) as b
    SELECT name FROM a
);

SELECT
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                explain,
                '_temporary_and_external_tables._tmp_\\w+\\-\\w+\\-\\w+\\-\\w+\\-\\w+',
                '_temporary_and_external_tables._tmp_UNIQ_ID'
            ),
            '_materialized_cte_\\w+\\_\\w+',
            '_materialized_cte_UNIQ_ID'
        ),
        '(\\bid|source_id): \\d+',
        '\\1: N'
    )
FROM
(
    EXPLAIN QUERY TREE
    WITH
    a AS MATERIALIZED (SELECT * FROM users)
    SELECT count() FROM a as l JOIN a as r ON l.uid = r.uid
);

SELECT
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                explain,
                '_temporary_and_external_tables._tmp_\\w+\\-\\w+\\-\\w+\\-\\w+\\-\\w+',
                '_temporary_and_external_tables._tmp_UNIQ_ID'
            ),
            '_materialized_cte_\\w+\\_\\w+',
            '_materialized_cte_UNIQ_ID'
        ),
        '(\\bid|source_id): \\d+',
        '\\1: N'
    )
FROM
(
    EXPLAIN QUERY TREE
    WITH
    a AS MATERIALIZED (SELECT uid, count() FROM users GROUP BY uid, uid + 1)
    SELECT count() FROM a as l JOIN a as r ON l.uid = r.uid
);

EXPLAIN header = 1
WITH
a AS MATERIALIZED (SELECT * FROM users)
SELECT count() FROM a as l JOIN a as r ON l.uid = r.uid;

WITH
a AS MATERIALIZED (SELECT * FROM users)
SELECT count() FROM a as l JOIN a as r ON l.uid = r.uid;

EXPLAIN
WITH
    cte AS MATERIALIZED (
        SELECT 10 AS n
        UNION ALL
        SELECT number as n FROM numbers(5)
    )
SELECT count() FROM cte, cte;

WITH
    cte AS MATERIALIZED (
        SELECT 10 AS n
        UNION ALL
        SELECT number as n FROM numbers(5)
    )
SELECT count() FROM cte, cte;

SELECT '-- Materialized CTE in scalar subquery';
WITH t AS MATERIALIZED (SELECT number AS c FROM numbers(10)), (SELECT max(c) FROM t) AS scalar
SELECT scalar + 1, scalar;
