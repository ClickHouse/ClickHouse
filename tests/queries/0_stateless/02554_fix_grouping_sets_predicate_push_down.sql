-- Tags: no-object-storage

DROP TABLE IF EXISTS test_grouping_sets_predicate;

CREATE TABLE test_grouping_sets_predicate
(
    day_ Date,
    type_1 String
)
ENGINE=MergeTree
ORDER BY day_;

INSERT INTO test_grouping_sets_predicate SELECT
    toDate('2023-01-05') AS day_,
    'hello, world'
FROM numbers (10);

SELECT '---Explain Syntax---';
EXPLAIN SYNTAX
SELECT *
FROM
(
    SELECT
        day_,
        if(type_1 = '', 'all', type_1) AS type_1
    FROM
    (
        SELECT
            day_,
            type_1
        FROM test_grouping_sets_predicate
        WHERE day_ = '2023-01-05'
        GROUP BY
            GROUPING SETS (
                (day_, type_1),
                (day_))
    ) AS t
)
WHERE type_1 = 'all';

SELECT '';
SELECT '---Explain Pipeline---';
EXPLAIN PIPELINE
SELECT *
FROM
(
    SELECT
        day_,
        if(type_1 = '', 'all', type_1) AS type_1
    FROM
    (
        SELECT
            day_,
            type_1
        FROM test_grouping_sets_predicate
        WHERE day_ = '2023-01-05'
        GROUP BY
            GROUPING SETS (
                (day_, type_1),
                (day_))
    ) AS t
)
WHERE type_1 = 'all' settings enable_analyzer=0;

-- Query plan with analyzer has less Filter steps (which is more optimal)
EXPLAIN PIPELINE
SELECT *
FROM
(
    SELECT
        day_,
        if(type_1 = '', 'all', type_1) AS type_1
    FROM
    (
        SELECT
            day_,
            type_1
        FROM test_grouping_sets_predicate
        WHERE day_ = '2023-01-05'
        GROUP BY
            GROUPING SETS (
                (day_, type_1),
                (day_))
    ) AS t
)
WHERE type_1 = 'all' settings enable_analyzer=1;

SELECT '';
SELECT '---Result---';
SELECT *
FROM
(
    SELECT
        day_,
        if(type_1 = '', 'all', type_1) AS type_1
    FROM
    (
        SELECT
            day_,
            type_1
        FROM test_grouping_sets_predicate
        WHERE day_ = '2023-01-05'
        GROUP BY
            GROUPING SETS (
                (day_, type_1),
                (day_))
    ) AS t
)
WHERE type_1 = 'all';

SELECT '';
SELECT '---Explain Pipeline---';
EXPLAIN PIPELINE
SELECT *
FROM
(
    SELECT
        day_,
        if(type_1 = '', 'all', type_1) AS type_1
    FROM
    (
        SELECT
            day_,
            type_1
        FROM test_grouping_sets_predicate
        GROUP BY
            GROUPING SETS (
                (day_, type_1),
                (day_))
    ) AS t
)
WHERE day_ = '2023-01-05' settings enable_analyzer=0;

-- Query plan with analyzer has less Filter steps (which is more optimal)
EXPLAIN PIPELINE
SELECT *
FROM
(
    SELECT
        day_,
        if(type_1 = '', 'all', type_1) AS type_1
    FROM
    (
        SELECT
            day_,
            type_1
        FROM test_grouping_sets_predicate
        GROUP BY
            GROUPING SETS (
                (day_, type_1),
                (day_))
    ) AS t
)
WHERE day_ = '2023-01-05' settings enable_analyzer=1;

DROP TABLE test_grouping_sets_predicate;
