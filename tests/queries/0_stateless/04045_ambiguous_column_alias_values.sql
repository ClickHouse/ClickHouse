SET enable_analyzer = 1;

-- https://github.com/ClickHouse/ClickHouse/issues/62566
SELECT
    *,
    'redefined' AS my_field
FROM VALUES('my_field String', 'orig');

SELECT
    my_field,
    'redefined' AS my_field
FROM VALUES('my_field String', 'orig');

SELECT
    t.my_field,
    'redefined' AS my_field
FROM VALUES('my_field String', 'orig') AS t;
