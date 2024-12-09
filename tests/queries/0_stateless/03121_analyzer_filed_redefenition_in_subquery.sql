-- https://github.com/ClickHouse/ClickHouse/issues/14739
SET enable_analyzer=1;

drop table if exists test_subquery;

CREATE TABLE test_subquery
ENGINE = Memory AS
SELECT 'base' AS my_field;

-- query 1
SELECT my_field
FROM
(
    SELECT
        *,
        'redefined' AS my_field
	from test_subquery
);

-- query 2
SELECT my_field
FROM
(
    SELECT
        'redefined' AS my_field,
        *
	from test_subquery
);

-- query 3
-- it works with old analyzer
SELECT my_field
FROM
(
    SELECT
        *,
        'redefined' AS my_field
	from (select * from test_subquery)
); -- {serverError AMBIGUOUS_COLUMN_NAME}
